
(* Trivial distributed key-value service.
 *
 * Usage:
 *
 * (1) Launch of 1st node (will be master with quorum = 1 at first):
 *
 *     ./dict -master  n1a,n1b
 *         (uses UNIX domain sockets  n1a for Raft communication,
 *          n1b as address for app server -- use   ip1:port1,ip2:port2
 *          to listen at ip1:port1 for Raft, ip1:port2 for app)
 *
 *
 * (2) Launch extra nodes. They will join the cluster and the quorum will be
 *     updated.
 *
 *     ./dict -master n2a,n2b -join n1a,n1b
 *
 *     ./dict -master n3a,n3b -join n1a,n1b
 *
 *     ...
 *
 * (3) perform client reqs
 *
 *     ./dict -client n1a,n1b -key foo             # retrieve value assoc'ed
 *                                                 # to key "foo", block until
 *                                                 # available
 *
 *     ./dict -client n1a,n1b -key foo -value bar  # associate 'bar' to 'foo'
 *
 * *)

open Lwt.Infix
open Bin_prot.Std

module String  = BatString
module Hashtbl = BatHashtbl
module Option  = BatOption

type op =
    Get of string
  | Wait of string
  | Set of string * string [@@deriving bin_io]

module CONF =
struct
  type nonrec op = op [@@deriving bin_io]

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
        | '!' ->
            let k, v = String.slice ~first:1 s |> String.split ~by:"=" in
              Set (k, v)
        | _ -> failwith "bad op"

  let sockaddr s =
    try
      let host, port = String.split ~by:":" s in
        Unix.ADDR_INET (Unix.inet_addr_of_string host, int_of_string port)
    with Not_found ->
      Unix.ADDR_UNIX s

  let node_sockaddr s = String.split ~by:"," s |> fst |> sockaddr
  let app_sockaddr  s =
    Printf.printf "Connecting to %s\n%!" s;
    String.split ~by:"," s |> snd |> sockaddr

  let string_of_address s = s
end

module SERVER = Oraft_rsm.Make_server(CONF)
module CLIENT = Oraft_rsm.Make_client(CONF)

let make_tls_wrapper tls =
  Option.map
    (fun (client_config, server_config) ->
       Oraft_lwt_tls.make_server_wrapper
         ~client_config ~server_config ())
    tls

let run_server ?tls ~addr ?join ~id () =
  let h    = Hashtbl.create 13 in
  let cond = Lwt_condition.create () in

  let exec _ op = match op with
      Get s -> `Sync (Lwt.return (try `OK (Hashtbl.find h s) with Not_found -> `OK ""))
    | Wait k ->
        `Async begin
          let rec attempt () =
            match Hashtbl.Exceptionless.find h k with
                Some v -> Lwt.return (`OK v)
              | None ->
                  Lwt_condition.wait cond >>= fun () ->
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
        `Sync (Lwt.return (`OK ""))
  in

  SERVER.make ?conn_wrapper:(make_tls_wrapper tls) exec addr ?join id >>=
  SERVER.run

let client_op ?tls ~addr op =
  let c    = CLIENT.make
               ?conn_wrapper:(make_tls_wrapper tls)
               ~id:(string_of_int (Unix.getpid ())) () in
  let exec = match op with
               | Get _ | Wait _ -> CLIENT.execute_ro
               | Set _ -> CLIENT.execute
  in
    CLIENT.connect c ~addr >>= fun () ->
    match%lwt exec c op with
      | `OK s -> Printf.printf "+OK %s\n" s; Lwt.return_unit
      | `Error s -> Printf.printf "-ERR %s\n" s; Lwt.return_unit

let ro_benchmark ?tls ?(iterations = 10_000) ~addr () =
  let c    = CLIENT.make
               ?conn_wrapper:(make_tls_wrapper tls)
               ~id:(string_of_int (Unix.getpid ())) ()
  in
    CLIENT.connect c ~addr >>= fun () ->
    CLIENT.execute c (Set ("bm", "0")) >>= fun _result ->
    let t0 = Unix.gettimeofday () in
      for%lwt i = 1 to iterations do
        let%lwt _ = CLIENT.execute_ro c (Get "bm") in
          Lwt.return_unit
      done >>= fun () ->
      let dt = Unix.gettimeofday () -. t0 in
        Printf.printf "%.0f RO ops/s\n" (float iterations /. dt);
        Lwt.return_unit

let wr_benchmark ?tls ?(iterations = 10_000) ~addr () =
  let c    = CLIENT.make
               ?conn_wrapper:(make_tls_wrapper tls)
               ~id:(string_of_int (Unix.getpid ())) ()
  in
    CLIENT.connect c ~addr >>= fun () ->
    let t0 = Unix.gettimeofday () in
      for%lwt i = 1 to iterations do
        let%lwt _ = CLIENT.execute c (Set ("bm", "")) in
          Lwt.return_unit
      done >>= fun () ->
      let dt = Unix.gettimeofday () -. t0 in
        Printf.printf "%.0f WR ops/s\n" (float iterations /. dt);
        Lwt.return_unit

let mode         = ref `Help
let cluster_addr = ref None
let k            = ref None
let v            = ref None
let ro_bm_iters  = ref 0
let wr_bm_iters  = ref 0
let tls          = ref None

let specs =
  Arg.align
    [
      "-tls", Arg.String (fun dirname -> tls := Some dirname),
      "dirname Directory containing PEM files";
      "-master", Arg.String (fun n -> mode := `Master n),
      "ADDR Launch master at given address";
      "-join", Arg.String (fun p -> cluster_addr := Some p),
      "ADDR Join cluster at given address";
      "-client", Arg.String (fun addr -> mode := `Client addr), "ADDR Client mode";
      "-key", Arg.String (fun s -> k := Some s), "STRING Wait for key/set it";
      "-value", Arg.String (fun s -> v := Some s),
      "STRING Set key given in -key to STRING";
      "-ro_bm", Arg.Set_int ro_bm_iters, "N Run RO benchmark (N iterations)";
      "-wr_bm", Arg.Set_int wr_bm_iters, "N Run WR benchmark (N iterations)";
    ]

let usage () =
  print_endline (Arg.usage_string specs "Usage:");
  exit 1

let x509_cert dirname = dirname ^ "/server.crt"
let x509_pk dirname   = dirname ^ "/server.key"

let tls_create dirname =
  let x509_cert   = x509_cert dirname in
  let x509_pk     = x509_pk dirname in
  let%lwt certificate =
    X509_lwt.private_of_pems ~cert:x509_cert ~priv_key:x509_pk in
    (* FIXME: authenticator *)
    Lwt.return (Some Tls.Config.(client ~authenticator:X509.Authenticator.null (),
                                 server ~certificates:(`Single certificate) ()))

let () =
  ignore (Sys.set_signal Sys.sigpipe Sys.Signal_ignore);
  Arg.parse specs ignore "Usage:";
  let tls = match !tls with
    | None -> Lwt.return None
    | Some dirname -> tls_create dirname
  in
    match !mode with
      | `Help -> usage ()
      | `Master addr ->
          Lwt_main.run (tls >>= fun tls ->
                        run_server ?tls ~addr ?join:!cluster_addr ~id:addr ())
      | `Client addr ->
          Printf.printf "Launching client %d\n" (Unix.getpid ());
          if !ro_bm_iters > 0 then
            Lwt_main.run (tls >>= fun tls ->
                          ro_benchmark ?tls ~iterations:!ro_bm_iters ~addr ());
          if !wr_bm_iters > 0 then
            Lwt_main.run (tls >>= fun tls ->
                          wr_benchmark ?tls ~iterations:!wr_bm_iters ~addr ());

          if !ro_bm_iters > 0 || !wr_bm_iters > 0 then exit 0;

          match !k, !v with
            | None, None | None, _ -> usage ()
            | Some k, Some v ->
                Lwt_main.run (tls >>= fun tls ->
                              client_op ?tls ~addr (Set (k, v)))
            | Some k, None ->
                Lwt_main.run (tls >>= fun tls ->
                              client_op ?tls ~addr (Wait k))

