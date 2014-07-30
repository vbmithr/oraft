open Printf
open Lwt

module String  = BatString
module Hashtbl = BatHashtbl

let section = Lwt_log.Section.make "dict"

let ipv6addr = Re_pcre.regexp "\\[(.*)\\]:([0-9]*)%?(.*)?"

type op =
    Get of string
  | Wait of string
  | Set of string * string

module CONF =
struct
  type op_ = op
  type op = op_

  let string_of_op = function
      Get v -> "?" ^ v
    | Wait v -> "<" ^ v
    | Set (k, v) -> "!" ^ k ^ "=" ^ v

  let op_of_string s =
    if s = "" then failwith "bad op"
    else
      match s.[0] with
          '?' -> Get (String.slice ~first:1 s)
        | '<' -> Wait (String.slice ~first:1 s)
        | '!' -> let k, v = String.slice ~first:1 s |> String.split ~by:"=" in
                   Set (k, v)
        | _ -> failwith "bad op"

  let sockaddr s =
    let open Unix in
    (* Identify the type of address *)
    if Re.execp ipv6addr s
    then (* IPv6 address *)
      match
        Re.(exec ipv6addr s |> get_all)
      with
      | [|_;host;port;""|] ->
        ADDR_INET (inet_addr_of_string host, int_of_string port)
      | [|_;host;port;zone|] ->
        ADDR_INET (inet_addr_of_string host, int_of_string port)
      | _ ->
        raise (Invalid_argument "invalid IPv6 address")
    else (* not an IPv6 address *)
    try
      let host, service = String.split ~by:":" s in
      match getaddrinfo host service [] with
      | [] -> raise (Invalid_argument "getaddrinfo returned no results")
      | h::_ -> h.ai_addr
    with Not_found ->
      Lwt_log.ign_warning_f ~section "Using UNIX domain socket %s" s;
      ADDR_UNIX s

  let node_sockaddr s = String.split ~by:"," s |> fst |> sockaddr
  let app_sockaddr  s =
    printf "Connecting to %s\n%!" s;
    String.split ~by:"," s |> snd |> sockaddr

  let string_of_address s = s
end

module SERVER = RSM.Make_server(CONF)
module CLIENT = RSM.Make_client(CONF)

let run_server ~addr ?join ~id () =
  let h    = Hashtbl.create 13 in
  let cond = Lwt_condition.create () in

  let exec _ op = match op with
      Get s -> `Sync (return (try `OK (Hashtbl.find h s) with Not_found -> `OK ""))
    | Wait k ->
        `Async begin
          let rec attempt () =
            match Hashtbl.Exceptionless.find h k with
                Some v -> return (`OK v)
              | None ->
                  Lwt_condition.wait cond >>
                  attempt ()
          in
            attempt ()
        end
    | Set (k, v) ->
        if v = "" then
          Hashtbl.remove h k
        else begin
          Hashtbl.add h k v;
          Lwt_condition.broadcast cond ();
        end;
        `Sync (return (`OK ""))
  in

  lwt server = SERVER.make exec addr ?join id in
    SERVER.run server

let client_op ~addr op =
  let c    = CLIENT.make ~id:(string_of_int (Unix.getpid ())) () in
  let exec = match op with
               | Get _ | Wait _ -> CLIENT.execute_ro
               | Set _ -> CLIENT.execute
  in
    CLIENT.connect c ~addr >>
    match_lwt exec c op with
        `OK s -> printf "+OK %s\n" s; return ()
      | `Error s -> printf "-ERR %s\n" s; return ()

let ro_benchmark ?(iterations = 10_000) ~addr () =
  let c    = CLIENT.make ~id:(string_of_int (Unix.getpid ())) () in
    CLIENT.connect c ~addr >>
    CLIENT.execute c (Set ("bm", "0")) >>
    let t0 = Unix.gettimeofday () in
      for_lwt i = 1 to iterations do
        lwt _ = CLIENT.execute_ro c (Get "bm") in
          return_unit
      done >>
      let dt = Unix.gettimeofday () -. t0 in
        printf "%.0f RO ops/s\n" (float iterations /. dt);
        return ()

let wr_benchmark ?(iterations = 10_000) ~addr () =
  let c    = CLIENT.make ~id:(string_of_int (Unix.getpid ())) () in
    CLIENT.connect c ~addr >>
    let t0 = Unix.gettimeofday () in
      for_lwt i = 1 to iterations do
        lwt _ = CLIENT.execute c (Set ("bm", "")) in
          return_unit
      done >>
      let dt = Unix.gettimeofday () -. t0 in
        printf "%.0f WR ops/s\n" (float iterations /. dt);
        return ()

let mode         = ref `Help
let cluster_addr = ref None
let mcast_addr   = ref None
let k            = ref None
let v            = ref None
let ro_bm_iters  = ref 0
let wr_bm_iters  = ref 0

let initialized  = ref false

let specs =
  Arg.align
    [
      "-master", Arg.String (fun n -> mode := `Master n),
        "ADDR Launch master at given address (<node_addr>:<node_port>,<app_addr:app_port>)";
      "-join", Arg.String (fun p -> cluster_addr := Some p),
        "ADDR Join cluster at given address (<node_addr>:<node_port>,<app_addr:app_port>)";
      "-mcast", Arg.String (fun p -> mcast_addr := Some p),
        "ADDR Join mcast group at given address (<mcast_addr>:<port>%<iface>)";
      "-client", Arg.String (fun addr -> mode := `Client addr), "ADDR Client mode";
      "-key", Arg.String (fun s -> k := Some s), "STRING Wait for key/set it";
      "-value", Arg.String (fun s -> v := Some s),
        "STRING Set key given in -key to STRING";
      "-ro_bm", Arg.Set_int ro_bm_iters, "N Run RO benchmark (N iterations)";
      "-wr_bm", Arg.Set_int wr_bm_iters, "N Run WR benchmark (N iterations)";
      "-v", Unit (fun () -> Lwt_log.(add_rule "dict" Info)), " Be verbose";
      "-vv", Unit (fun () -> Lwt_log.(add_rule "dict" Debug)), " Be more verbose"
    ]

let usage () =
  print_endline (Arg.usage_string specs "Usage:");
  exit 1

let set_template () =
  let template = "$(date).$(milliseconds) [$(pid)]: $(message)" in
  let std_logger =
    Lwt_log.channel ~template ~close_mode:`Keep ~channel:Lwt_io.stdout () in
  Lwt_log.default := std_logger

let () = set_template ()

let get_peer_info = function
  | Unix.ADDR_UNIX _ -> raise_lwt (Invalid_argument "get_peer_info")
  | Unix.ADDR_INET (a, p) as sa ->
    (* Found one neighbour, asking his oraft node port *)
    let sa_domain = Unix.domain_of_sockaddr sa in
    let s = Lwt_unix.(socket sa_domain SOCK_STREAM 0) in
    Lwt_unix.connect s sa >>= fun () ->
    let buf = String.make 5 '\000' in
    Lwt_unix.recv s buf 0 5 [] >>= fun nb_recv ->
    if nb_recv <> 5
    then raise_lwt (Failure "get_peer_info: failed to obtain info")
    else
      let remote_node_port = EndianString.BigEndian.get_int16 buf 0 in
      let remote_app_port = EndianString.BigEndian.get_int16 buf 2 in
      let uint16_of_int16 i16 = if i16 < 0 then i16 + 65535 else i16 in
      let remote_node_port = uint16_of_int16 remote_node_port in
      let remote_app_port = uint16_of_int16 remote_app_port in
      let cluster_addr =
        match sa_domain with
        | Unix.PF_UNIX -> assert false
        | Unix.PF_INET ->
          Printf.sprintf "%s:%d,%s:%d"
            (Unix.string_of_inet_addr a) remote_node_port
            (Unix.string_of_inet_addr a) remote_app_port
        | Unix.PF_INET6 ->
          Printf.sprintf "[%s]:%d,[%s]:%d"
            (Unix.string_of_inet_addr a) remote_node_port
            (Unix.string_of_inet_addr a) remote_app_port
      in
      Lwt.return (cluster_addr, (buf.[4] <> '\000'))

let () =
  ignore (Sys.set_signal Sys.sigpipe Sys.Signal_ignore);
  Arg.parse specs ignore "Usage:";
  match !mode with
      `Help -> usage ()
    | `Master addr ->
      let my_node_sockaddr = CONF.node_sockaddr addr in
      let my_app_sockaddr = CONF.app_sockaddr addr in
      let my_ports = match my_node_sockaddr, my_app_sockaddr with
        | Unix.ADDR_INET (a, p),  Unix.ADDR_INET (a2, p2) -> p, p2
        | _ -> failwith "my_node_port" in
      (match !mcast_addr with
       | None -> Lwt_main.run (run_server ~addr ?join:!cluster_addr ~id:addr ())
       | Some mcast_addr ->
         if not (Re.execp ipv6addr mcast_addr) then
           raise (Invalid_argument "Invalid multicast address");
         match Re.(exec ipv6addr mcast_addr |> get_all) with
         | [|_;v6addr;port;iface|] ->
           let port = int_of_string port in
           let return_oraft_ports _ fd saddr =
             let buf = String.make 5 '\000' in
             EndianString.BigEndian.set_int16 buf 0 (fst my_ports);
             EndianString.BigEndian.set_int16 buf 2 (snd my_ports);
             if !initialized then
               buf.[4] <- '\001';
             Lwt_unix.send fd buf 0 5 [] >>= fun nb_sent ->
             if nb_sent <> 5 then
               Lwt_log.warning_f ~section "Could not send all info to %s"
                 (Llnet.Helpers.string_of_saddr saddr)
             else
               Lwt.return_unit
           in
           let main_thread () =
             Llnet.connect
               ~tcp_reactor:return_oraft_ports
               ~iface (Ipaddr.of_string_exn v6addr) port >>= fun h ->
             (* Waiting for other peers to manifest themselves *)
             Lwt_log.info_f ~section "I am %s, now detecting peers..."
               Llnet.(Helpers.string_of_saddr h.tcp_in_saddr)
             >>= fun () ->
             Lwt_unix.sleep (2. *. h.ival) >>= fun () ->
             let neighbours = Llnet.neighbours_nonblock h in
             let nb_neighbours = List.length neighbours in
             match nb_neighbours,
                   Llnet.order h,
                   Llnet.neighbours_nonblock h with
             | 0, _, _ ->
               (* We are alone, run server without joining a cluster *)
               initialized := true;
               run_server ~addr ?join:!cluster_addr ~id:addr ()
             | _, o, ns ->
               let rec try_joining_cluster () =
                 Lwt_list.map_p (fun n -> get_peer_info n) ns >>= fun p_infos ->
                 let initialized_peers =
                   List.fold_left (fun a (addr, init) ->
                       if init then addr::a else a) [] p_infos in
                 match initialized_peers with
                 | [] ->
                   (* No peers initialized, and I'm the lowest IP, run
                        server without joining a cluster *)
                   if o = 0 then
                     (
                       initialized := true;
                       Lwt_log.ign_info ~section "Found 0 peers initialized, running standalone";
                       run_server ~addr ?join:!cluster_addr ~id:addr ()
                     )
                   else Lwt_unix.sleep (2. *. h.ival) >>= fun () ->
                     try_joining_cluster ()
                 | peers ->
                   (* Some peers initialized, connecting to the first one *)
                   Lwt_list.iter_s (fun p ->
                       try_lwt
                         initialized := true;
                         Lwt_log.ign_info_f ~section "Connecting to %s" p;
                         run_server ~addr ?join:(Some p) ~id:addr ()
                       with exn ->
                         initialized := false;
                         Lwt_log.warning_f ~exn ~section
                           "Exn raised when trying to sync to peer %s, trying others"
                           p
                     ) peers >>= fun () ->
                   Lwt_unix.sleep (2. *. h.ival) >>= fun () ->
                   try_joining_cluster ()
               in try_joining_cluster ()
           in Lwt_main.run (main_thread ())
         | _ -> failwith "Invalid multicast address: zone id missing"

      )
    | `Client addr ->
      printf "Launching client %d\n" (Unix.getpid ());
      if !ro_bm_iters > 0 then
        Lwt_unix.run (ro_benchmark ~iterations:!ro_bm_iters ~addr ());
      if !wr_bm_iters > 0 then
        Lwt_unix.run (wr_benchmark ~iterations:!wr_bm_iters ~addr ());

      if !ro_bm_iters > 0 || !wr_bm_iters > 0 then exit 0;

      match !k, !v with
        None, None | None, _ -> usage ()
      | Some k, Some v ->
        Lwt_unix.run (client_op ~addr (Set (k, v)))
      | Some k, None ->
        Lwt_unix.run (client_op ~addr (Wait k))

