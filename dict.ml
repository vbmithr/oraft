open Printf
open Lwt

module String  = BatString
module Hashtbl = BatHashtbl

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
    try
      let host, service = String.split ~by:":" s in
      match getaddrinfo host service [] with
      | [] -> raise (Invalid_argument "getaddrinfo returned no results")
      | h::_ -> h.ai_addr
    with Not_found ->
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
let k            = ref None
let v            = ref None
let ro_bm_iters  = ref 0
let wr_bm_iters  = ref 0

let specs =
  Arg.align
    [
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

let () =
  ignore (Sys.set_signal Sys.sigpipe Sys.Signal_ignore);
  Arg.parse specs ignore "Usage:";
  match !mode with
      `Help -> usage ()
    | `Master addr ->
        Lwt_unix.run (run_server ~addr ?join:!cluster_addr ~id:addr ())
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

