open Lwt.Infix
open Sexplib.Std
open Bin_prot.Std

open Oraft.Types

let section = Lwt_log.Section.make "RSM"

let s_of_simple_config string_of_address l =
  List.map
    (fun (id, addr) -> Printf.sprintf "%S:%S" id (string_of_address addr)) l |>
  String.concat "; "

let string_of_config string_of_address c =
  match c with
      Simple_config (c, passive) ->
        Printf.sprintf "Simple ([%s], [%s])"
          (s_of_simple_config string_of_address c)
          (s_of_simple_config string_of_address passive)
    | Joint_config (c1, c2, passive) ->
        Printf.sprintf "Joint ([%s], [%s], [%s])"
          (s_of_simple_config string_of_address c1)
          (s_of_simple_config string_of_address c2)
          (s_of_simple_config string_of_address passive)

module Map     = BatMap
module Hashtbl = BatHashtbl
module Option  = BatOption

type 'a conn =
    {
      addr    : 'a;
      ich     : Lwt_io.input_channel;
      och     : Lwt_io.output_channel;
      in_buf  : Lwt_bytes.t;
      out_buf : Lwt_bytes.t;
    }

let send_msg conn write msg =
  (* Since the buffers are private to the conn AND Lwt_io.atomic prevents
   * concurrent IO operations, it's safe to reuse the buffer for a given
   * channel across send_msg calls.  *)
  Lwt_io.atomic begin fun och ->
    let msglen = write conn.out_buf ~pos:0 msg in
      Lwt_log.debug_f ~section "send_msg: %d bytes" msglen >>= fun () ->
      Lwt_io.LE.write_int och msglen >>= fun () ->
      Lwt_io.direct_access
        och (Oraft_lwt.da_write_msg conn.out_buf 0 msglen)
  end conn.och

let read_msg conn read =
  (* same as send_msg applies here regarding the buffers *)
  Lwt_io.atomic begin fun ich ->
    Lwt_io.LE.read_int ich >>= fun msglen ->
    Lwt_log.debug_f ~section "read_msg: %d bytes" msglen >>= fun () ->
    Lwt_io.direct_access
      ich (Oraft_lwt.da_read_msg conn.in_buf 0 msglen) >>= fun () ->
    Lwt.return (read conn.in_buf ~pos_ref:(ref 0))
  end conn.ich

type config_change =
    Add_failover of rep_id * address
  | Remove_failover of rep_id
  | Decommission of rep_id
  | Demote of rep_id
  | Promote of rep_id
  | Replace of rep_id * rep_id [@@deriving sexp,bin_io]

module type CONF =
sig
  include Oraft_lwt.SERVER_CONF
  val app_sockaddr : address -> Unix.sockaddr
end

type client_op =
    Connect of string
  | Execute of string
  | Execute_RO of string
  | Change_config of config_change
  | Get_config [@@deriving sexp,bin_io]

type response =
    OK of string
  | Redirect of (rep_id * address)
  | Retry
  | Cannot_change
  | Unsafe_change of (simple_config * passive_peers)
  | Error of string
  | Config of config [@@deriving sexp,bin_io]

type client_msg = { id : int64; op : client_op } [@@deriving sexp,bin_io]
type server_msg = { id : int64; response : response } [@@deriving sexp,bin_io]

module Make_client(C : CONF) =
struct
  module M = Map.Make(String)

  exception Not_connected
  exception Bad_response

  module H = Hashtbl.Make(struct
                            type t = Int64.t
                            let hash          = Hashtbl.hash
                            let equal i1 i2 = Int64.compare i1 i2 = 0
                          end)

  type t =
      {
        id             : string;
        mutable dst    : address conn option;
        mutable conns  : address conn M.t;
        mutable req_id : Int64.t;
        pending_reqs   : response Lwt.u H.t;
        conn_wrapper   : [`Outgoing] Oraft_lwt.conn_wrapper;
      }

  and address = string

  let trivial_wrapper () =
    (Oraft_lwt.trivial_conn_wrapper () :> [`Outgoing] Oraft_lwt.conn_wrapper)

  let make ?conn_wrapper ~id () =
    { id; dst = None; conns = M.empty; req_id = 0L;
      pending_reqs = H.create 13;
      conn_wrapper = Option.map_default
                       (fun w -> (w :> [`Outgoing] Oraft_lwt.conn_wrapper))
                       (trivial_wrapper ())
                       conn_wrapper;
    }

  let gen_id t =
    t.req_id <- Int64.succ t.req_id;
    t.req_id

  let send_msg conn msg = send_msg conn bin_write_client_msg msg
  let read_msg conn     = read_msg conn bin_read_server_msg

  let connect t peer_id addr =
    let do_connect () =
      let saddr = C.app_sockaddr addr in
      let fd = Lwt_unix.socket (Unix.domain_of_sockaddr saddr) Unix.SOCK_STREAM 0 in
      let%lwt () = Lwt_unix.connect fd saddr in
      let%lwt ich, och     = Oraft_lwt.wrap_outgoing_conn t.conn_wrapper fd in
      let out_buf      = Lwt_bytes.create 4096 in
      let in_buf       = Lwt_bytes.create 4096 in
      let conn         = { addr; ich; och; in_buf; out_buf } in
        (try Lwt_unix.setsockopt fd Unix.TCP_NODELAY true with _ -> ());
        (try Lwt_unix.setsockopt fd Unix.SO_KEEPALIVE true with _ -> ());
        try%lwt
          send_msg conn { id = 0L; op = (Connect t.id) } >>= fun () ->
          match%lwt read_msg conn with
            | { response = OK id; _ } ->
                t.conns <- M.add id conn t.conns;
                t.dst <- Some conn;
                ignore begin
                  let rec loop_recv () =
                    let%lwt msg = read_msg conn in
                      Lwt_log.debug_f ~section "<- loop_recv %s"
                        (Sexplib.Sexp.to_string_hum
                           (sexp_of_server_msg msg)) >>= fun () ->
                      match H.Exceptionless.find t.pending_reqs msg.id with
                          None -> loop_recv ()
                        | Some u ->
                            Lwt.wakeup_later u msg.response;
                            loop_recv ()
                  in
                    (loop_recv ())
                      [%finally
                        t.conns <- M.remove peer_id t.conns;
                        Lwt_io.abort och]
                end;
                Lwt.return_unit
            | _ -> failwith "conn refused"
        with _ ->
          t.conns <- M.remove peer_id t.conns;
          Lwt_io.abort och
    in
      match M.Exceptionless.find peer_id t.conns with
          Some conn when conn.addr = addr ->
            t.dst <- Some conn;
            Lwt.return_unit
        | Some conn (* when addr <> address *) ->
            t.conns <- M.remove peer_id t.conns;
            Lwt_io.abort conn.och >>= fun () -> do_connect ()
        | None -> do_connect ()

  let send_and_await_response t op f =
    match t.dst with
        None -> Lwt.fail Not_connected
      | Some c ->
          let th, u = Lwt.task () in
          let id    = gen_id t in
          let msg   = { id; op; } in
            H.add t.pending_reqs id u;
            let msg_sexp_str = Sexplib.Sexp.to_string_hum (sexp_of_client_msg msg) in
              Lwt_log.debug_f ~section "-> %s" msg_sexp_str >>= fun () ->
              send_msg c msg >>= fun () ->
              let%lwt x = th in
                f c.addr x

  let rec do_execute t op =
    send_and_await_response t op begin fun dst resp ->
      Lwt_log.debug_f ~section "<- do_execute %s"
        (Sexplib.Sexp.to_string_hum (sexp_of_response resp)) >>= fun () ->
      match resp with
          OK s -> Lwt.return (`OK s)
        | Error s -> Lwt.return (`Error s)
        | Redirect (peer_id, address) when peer_id <> dst ->
            connect t peer_id address >>= fun () ->
            do_execute t op
        | Redirect _ | Retry ->
            Lwt_unix.sleep 0.050 >>= fun () ->
            do_execute t op
        | Cannot_change | Unsafe_change _ | Config _ ->
            Lwt.fail Bad_response
    end

  let execute t op =
    do_execute t (Execute (C.string_of_op op))

  let execute_ro t op =
    do_execute t (Execute_RO (C.string_of_op op))

  let rec get_config t =
    send_and_await_response t Get_config
      (fun dst resp -> match resp with
           Config c -> Lwt.return (`OK c)
         | Error x -> Lwt.return (`Error x)
         | Redirect (peer_id, address) when peer_id <> dst ->
             connect t peer_id address >>= fun () ->
             get_config t
         | Redirect _ | Retry ->
             Lwt_unix.sleep 0.050 >>= fun () ->
             get_config t
         | OK _ | Cannot_change | Unsafe_change _ ->
             Lwt.fail Bad_response)

  let rec change_config t op =
    send_and_await_response t (Change_config op)
      (fun dst resp -> match resp with
           OK _ -> Lwt.return `OK
         | Error x -> Lwt.return (`Error x)
         | Redirect (peer_id, address) when peer_id <> dst ->
             connect t peer_id address >>= fun () ->
             change_config t op
         | Redirect _ | Retry ->
             Lwt_unix.sleep 0.050 >>= fun () ->
             change_config t op
         | Cannot_change -> Lwt.return (`Cannot_change)
         | Unsafe_change (c, p) -> Lwt.return (`Unsafe_change (c, p))
         | Config _ -> Lwt.fail Bad_response)

  let connect t ~addr = connect t "" addr
end

module Make_server(C : CONF) =
struct
  module SS   = Oraft_lwt.Simple_server(C)
  module SSC  = SS.Config
  module CC   = Make_client(C)

  module Core = SS

  type 'a execution = [`Sync of 'a Lwt.t | `Async of 'a Lwt.t]
  type 'a apply     = 'a Core.server -> C.op -> [`OK of 'a | `Error of exn] execution

  type 'a t =
      {
        id            : rep_id;
        addr          : string;
        c             : CC.t option;
        node_sockaddr : Unix.sockaddr;
        app_sockaddr  : Unix.sockaddr;
        serv          : 'a SS.server;
        exec          : 'a SS.apply;
        conn_wrapper  : [`Incoming | `Outgoing] Oraft_lwt.conn_wrapper;
      }

  let raise_if_error = function
      `OK x -> Lwt.return x
    | `Error s -> Lwt.fail_with s

  let check_config_err = function
    | `OK -> Lwt.return_unit
    | `Error s -> Lwt.fail_with s
    | `Cannot_change -> Lwt.fail_with "Cannot perform config change"
    | `Unsafe_change _ -> Lwt.fail_with "Unsafe config change"

  let make exec addr
          ?(conn_wrapper = Oraft_lwt.trivial_conn_wrapper ())
          ?join ?election_period ?heartbeat_period id =
    match join with
      | None ->
          let config        = Simple_config ([id, addr], []) in
          let state         = Oraft.Core.make
                                ~id ~current_term:0L ~voted_for:None
                                ~log:[] ~config () in
          let node_sockaddr = C.node_sockaddr addr in
          let app_sockaddr  = C.app_sockaddr addr in
          SS.make_conn_manager ~id node_sockaddr >>= fun conn_mgr ->
          let serv          = SS.make exec ?election_period ?heartbeat_period
                                state conn_mgr
          in
            Lwt.return { id; addr; c = None; node_sockaddr;
                         app_sockaddr; serv; exec; conn_wrapper; }
      | Some peer_addr ->
          let c = CC.make ~conn_wrapper ~id () in
            Lwt_log.info_f ~section "Connecting to %S" (peer_addr |> C.string_of_address) >>= fun () ->
            CC.connect c ~addr:peer_addr >>= fun () ->
            let%lwt config        = CC.get_config c >>= raise_if_error in
            let%lwt ()            = Lwt_log.info_f ~section "Got initial configuration %s"
                                  (string_of_config C.string_of_address config) in
            let state         = Oraft.Core.make
                                  ~id ~current_term:0L ~voted_for:None
                                  ~log:[] ~config () in
            let node_sockaddr = C.node_sockaddr addr in
            let app_sockaddr  = C.app_sockaddr addr in
            SS.make_conn_manager ~id node_sockaddr >>= fun conn_mgr ->
            let serv          = SS.make exec ?election_period
                                  ?heartbeat_period state conn_mgr
            in
              Lwt.return { id; addr; c = Some c;
                           node_sockaddr; app_sockaddr; serv; exec; conn_wrapper }

  let send_msg conn msg = send_msg conn bin_write_server_msg msg
  let read_msg conn     = read_msg conn bin_read_client_msg

  let map_op_result = function
    | `Redirect (peer_id, addr) -> Redirect (peer_id, addr)
    | `Retry -> Retry
    | `Error exn -> Error (Printexc.to_string exn)
    | `OK s -> OK s

  let perform_change t op =
    let map = function
          `OK -> OK ""
        | `Cannot_change -> Cannot_change
        | `Unsafe_change (c, p) -> Unsafe_change (c, p)
        | `Redirect _ | `Retry as x -> map_op_result x
    in
      try%lwt
        let%lwt ret =
          match op with
              Add_failover (peer_id, addr) -> SSC.add_failover t.serv peer_id addr
            | Remove_failover peer_id -> SSC.remove_failover t.serv peer_id
            | Decommission peer_id -> SSC.decommission t.serv peer_id
            | Demote peer_id -> SSC.demote t.serv peer_id
            | Promote peer_id -> SSC.promote t.serv peer_id
            | Replace (replacee, failover) -> SSC.replace t.serv ~replacee ~failover
        in
          Lwt.return (map ret)
      with exn ->
        Lwt_log.debug_f ~section ~exn
          "Error while changing cluster configuration" >>= fun () ->
        Lwt.return (Error (Printexc.to_string exn))

  let process_message t _client_id conn = function
      { id; op = Connect _ } ->
        send_msg conn { id; response = Error "Unexpected request" }
    | { id; op = Get_config } ->
        let config = SS.Config.get t.serv in
          send_msg conn { id; response = Config config }
    | { id; op = Change_config x } ->
        Lwt_log.info_f ~section
          "Config change requested" >>= fun () ->
        let%lwt response = perform_change t x in
          Lwt_log.info_f ~section
            "Config change result" >>= fun () ->
          Lwt_log.info_f ~section
            "New config: %s"
            (string_of_config C.string_of_address (SS.config t.serv)) >>= fun () ->
          send_msg conn { id; response }
    | { id; op = Execute_RO op; } -> begin
        match%lwt SS.readonly_operation t.serv with
          | `Redirect _ | `Retry | `Error _ as x ->
              let response = map_op_result x in
                send_msg conn { id; response; }
          | `OK ->
              match t.exec t.serv (C.op_of_string op) with
                | `Sync resp ->
                    let%lwt resp = resp in
                      send_msg conn { id; response = map_op_result resp }
                | `Async resp ->
                    ignore begin try%lwt
                      let%lwt resp = try%lwt resp
                                 with exn -> Lwt.return (`Error exn)
                      in
                        send_msg conn { id; response = map_op_result resp }
                    with exn ->
                      Lwt_log.debug ~section ~exn "Caught exn"
                    end;
                    Lwt.return_unit
      end
    | { id; op = Execute op; } ->
        let%lwt response = SS.execute t.serv (C.op_of_string op) >|= map_op_result in
          send_msg conn { id; response }

  let rec request_loop t client_id conn =
    let%lwt msg = read_msg conn in
      ignore begin
        try%lwt
          process_message t client_id conn msg
        with exn ->
          Lwt_log.debug_f ~section ~exn
            "Error while processing message" >>= fun () ->
          send_msg conn { id = msg.id; response = Error (Printexc.to_string exn) }
      end;
      request_loop t client_id conn

  let is_in_config t config =
    let all = match config with
      | Simple_config (a, p) -> a @ p
      | Joint_config (a1, a2, p) -> a1 @ a2 @ p
    in
      List.mem_assoc t.id all

  let is_active t config =
    let active = match config with
      | Simple_config (a, _) -> a
      | Joint_config (a1, a2, _) -> a1 @ a2
    in
      List.mem_assoc t.id active

  let add_as_failover_if_needed t c config =
    if is_in_config t config then
      Lwt.return_unit
    else begin
      Lwt_log.info_f ~section "Adding failover id:%S addr:%S"
        t.id (C.string_of_address t.addr)>>= fun () ->
      CC.change_config c (Add_failover (t.id, t.addr)) >>= check_config_err
    end

  let promote_if_needed t c config =
    if is_active t config then
      Lwt.return_unit
    else begin
      Lwt_log.info_f ~section "Promoting failover id:%S" t.id>>= fun () ->
      CC.change_config c (Promote t.id) >>= check_config_err
    end

  let join_cluster t c =
    (* We only try to add as failover/promote if actually needed.
     * Otherwise, we could get blocked in situations were the node is
     * rejoining the cluster (and thus already active in its configuration)
     * and the remaining nodes do not have the quorum to perform a
     * configuration change (even if it'd eventually be a NOP). *)
    let%lwt config = CC.get_config c >>= raise_if_error in
      add_as_failover_if_needed t c config>>= fun () ->
      promote_if_needed t c config>>= fun () ->
      let%lwt config = CC.get_config c >>= raise_if_error in
        Lwt_log.info_f ~section "Final config: %s"
          (string_of_config C.string_of_address config)

  let handle_conn t fd addr =
    (* the following are not supported for ADDR_UNIX sockets, so catch *)
    (* possible exceptions  *)
    (try Lwt_unix.setsockopt fd Unix.TCP_NODELAY true with _ -> ());
    (try Lwt_unix.setsockopt fd Unix.SO_KEEPALIVE true with _ -> ());
    (try%lwt
      let%lwt ich, och = Oraft_lwt.wrap_incoming_conn t.conn_wrapper fd in
      let conn     = { addr; ich; och;
                       in_buf = Lwt_bytes.create 4096;
                       out_buf = Lwt_bytes.create 4096 } in
        (match%lwt read_msg conn with
            | { id; op = Connect client_id; _ } ->
                Lwt_log.info_f ~section
                  "Incoming client connection from %S" client_id>>= fun () ->
                send_msg conn { id; response = OK "" }>>= fun () ->
                request_loop t client_id conn
            | { id; _ } ->
                send_msg conn { id; response = Error "Bad request" })
     [%finally
       try%lwt Lwt_io.close ich with _ -> Lwt.return_unit]
     with
      | End_of_file
      | Unix.Unix_error (Unix.ECONNRESET, _, _) -> Lwt.return_unit
      | exn ->
          Lwt_log.error_f ~section ~exn "Error in dispatch")
      [%finally
        try%lwt Lwt_unix.close fd with _ -> Lwt.return_unit]

  let run t =
    let sock = Lwt_unix.(socket (Unix.domain_of_sockaddr t.app_sockaddr)
                           Unix.SOCK_STREAM 0)
    in
      Lwt_unix.setsockopt sock Unix.SO_REUSEADDR true;
      Lwt_unix.bind sock t.app_sockaddr >>= fun () ->
      Lwt_unix.listen sock 256;

      let rec accept_loop t =
        let%lwt (fd_addrs,_) = Lwt_unix.accept_n sock 50 in
          List.iter
            (fun (fd, addr) -> Lwt.async (fun () -> handle_conn t fd addr))
            fd_addrs;
          accept_loop t
      in
        ignore begin try%lwt
            Lwt_log.info_f ~section "Running app server at %s"
              (match t.app_sockaddr with
                | Unix.ADDR_INET (a, p) -> Printf.sprintf "%s/%d" (Unix.string_of_inet_addr a) p
                | Unix.ADDR_UNIX s -> Printf.sprintf "unix://%s" s)>>= fun () ->
            SS.run t.serv
          with exn ->
            Lwt_log.error_f ~section ~exn "Error in Oraft_lwt server run()"
        end;
        (try%lwt
          match t.c with
            | None -> accept_loop t
            | Some c -> join_cluster t c >>= fun () -> accept_loop t
        with exn ->
          Lwt_log.fatal ~section ~exn "Exn raised")
          [%finally
             (* FIXME: t.c client shutdown *)
            try%lwt Lwt_unix.close sock with _ -> Lwt.return_unit]
end
