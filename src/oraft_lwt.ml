open Lwt.Infix
open Oraft
open Oraft.Types

module Map    = BatMap
module List   = BatList
module Option = BatOption
module Queue  = BatQueue

let src = Logs.Src.create "oraft_lwt"

let pp_exn ppf exn =
  Format.pp_print_string ppf (Printexc.to_string exn)

let pp_saddr ppf = function
  | Unix.ADDR_INET (a, p) ->
      Format.fprintf ppf "%s/%d" (Unix.string_of_inet_addr a) p
  | Unix.ADDR_UNIX s ->
      Format.fprintf ppf "unix://%s" s

let rec da_write_msg buf pos len (da : Lwt_io.direct_access) =
  let avail = da.da_max - da.da_ptr in
    if len <= avail then begin
      Lwt_bytes.blit buf pos da.da_buffer da.da_ptr len ;
      da.da_ptr <- da.da_ptr + len ;
      Lwt.return_unit
    end
    else begin
      Lwt_bytes.blit buf pos da.da_buffer da.da_ptr avail ;
      da.da_ptr <- da.da_max ;
      da.da_perform () >>= fun _nb_written ->
      da_write_msg buf (pos + avail) (len - avail) da
    end

let rec da_read_msg buf pos len (da : Lwt_io.direct_access) =
  if len <= 0 then Lwt.return_unit
  else
    let readable = da.da_max - da.da_ptr in
      if readable = 0 then
        da.da_perform () >>= fun _nb_read ->
        da_read_msg buf pos len da
      else
        let nb_to_read = min len readable in
          Lwt_bytes.blit da.da_buffer da.da_ptr buf pos nb_to_read ;
          da.da_ptr <- da.da_ptr + nb_to_read ;
          da_read_msg buf (pos + nb_to_read) (len - nb_to_read) da

module REPID = struct type t = rep_id let compare = String.compare end
module RM = Map.Make(REPID)
module RS = Set.Make(REPID)

module type LWTIO_TYPES =
sig
  type op
  type connection
  type conn_manager
end

module type LWTIO =
sig
  include LWTIO_TYPES

  val connect : conn_manager -> rep_id -> address -> connection option Lwt.t
  val send    : connection -> (req_id * op) message -> unit Lwt.t
  val receive : connection -> (req_id * op) message option Lwt.t
  val abort   : connection -> unit Lwt.t

  val is_saturated : connection -> bool

  type snapshot_transfer

  val prepare_snapshot :
    connection -> index -> config -> snapshot_transfer option Lwt.t

  val send_snapshot : snapshot_transfer -> bool Lwt.t
end

module type SERVER_GENERIC =
sig
  open Oraft.Types

  include LWTIO_TYPES

  type 'a server

  type gen_result =
      [ `Error of exn
      | `Redirect of rep_id * address
      | `Retry ]

  type 'a cmd_result   = [ gen_result | `OK of 'a ]
  type ro_op_result = [ gen_result | `OK ]

  type 'a execution = [`Sync of 'a Lwt.t | `Async of 'a Lwt.t]
  type 'a apply     = 'a server -> op -> [`OK of 'a | `Error of exn] execution

  val make :
    'a apply -> ?election_period:float -> ?heartbeat_period:float ->
    (req_id * op) Oraft.Core.state -> conn_manager -> 'a server

  val config  : _ server -> config
  val run     : _ server -> unit Lwt.t
  val abort   : _ server -> unit Lwt.t
  val execute : 'a server -> op -> 'a cmd_result Lwt.t
  val readonly_operation : _ server -> ro_op_result Lwt.t

  val compact_log : _ server -> index -> unit

  module Config :
  sig
    type result =
      [
      | `OK
      | `Redirect of rep_id * address
      | `Retry
      | `Cannot_change
      | `Unsafe_change of simple_config * passive_peers
      ] [@@deriving bin_io]

    val get             : _ server -> config
    val add_failover    : _ server -> rep_id -> address -> result Lwt.t
    val remove_failover : _ server -> rep_id -> result Lwt.t
    val decommission    : _ server -> rep_id -> result Lwt.t
    val demote          : _ server -> rep_id -> result Lwt.t
    val promote         : _ server -> rep_id -> result Lwt.t
    val replace         : _ server -> replacee:rep_id -> failover:rep_id -> result Lwt.t
  end
end

let retry_delay = 0.05

module Make_server(IO : LWTIO) =
struct
  module S    = Set.Make(String)
  module CMDM = Map.Make(struct
                           type t = req_id
                           let compare = compare
                         end)

  exception Stop_node

  type op           = IO.op
  type connection   = IO.connection
  type conn_manager = IO.conn_manager

  type 'a execution = [`Sync of 'a Lwt.t | `Async of 'a Lwt.t]
  type 'a apply     = 'a server -> op -> [`OK of 'a | `Error of exn] execution

  and 'a server =
      {
        execute                  : 'a apply;
        conn_manager             : IO.conn_manager;
        election_period          : float;
        heartbeat_period         : float;
        mutable next_req_id      : Int64.t;
        mutable conns            : IO.connection RM.t;
        mutable connecting       : RS.t;
        mutable state            : (req_id * IO.op) Core.state;
        mutable running          : bool;
        msg_stream               : (rep_id * (req_id * IO.op) message) Lwt_stream.t;
        push_msg                 : rep_id * (req_id * IO.op) message -> unit;
        mutable get_msg          : th_res Lwt.t;
        mutable election_timeout : th_res Lwt.t;
        mutable heartbeat        : th_res Lwt.t;
        mutable abort            : th_res Lwt.t * th_res Lwt.u;
        mutable get_cmd          : th_res Lwt.t;
        mutable get_ro_op        : th_res Lwt.t;
        push_cmd                 : (req_id * IO.op) -> unit;
        cmd_stream               : (req_id * IO.op) Lwt_stream.t;
        push_ro_op               : ro_op_res Lwt.u -> unit;
        ro_op_stream             : ro_op_res Lwt.u Lwt_stream.t;
        pending_ro_ops           : (Int64.t * ro_op_res Lwt.u) Queue.t;
        mutable pending_cmds     : ('a cmd_res Lwt.t * 'a cmd_res Lwt.u) CMDM.t;
        leader_signal            : unit Lwt_condition.t;
        sent_snapshots           : (rep_id * index) Lwt_stream.t;
        mutable sent_snapshots_th  : th_res Lwt.t;
        snapshot_sent            : ((rep_id * index) -> unit);
        failed_snapshots         : rep_id Lwt_stream.t;
        mutable failed_snapshot_th : th_res Lwt.t;
        snapshot_failed          : rep_id -> unit;
        mutable config_change    : config_change;

        apply_stream             : (req_id * IO.op) Lwt_stream.t;
        push_apply               : (req_id * IO.op) -> unit;
      }

  and config_change =
    | No_change
    | New_failover of change_result Lwt.u * rep_id * address
    | Remove_failover of change_result Lwt.u * rep_id
    | Decommission of change_result Lwt.u * rep_id
    | Promote of change_result Lwt.u * rep_id
    | Demote of change_result Lwt.u * rep_id
    | Replace of change_result Lwt.u * rep_id * rep_id

  and change_result = OK | Retry

  and th_res =
      Message of rep_id * (req_id * IO.op) message
    | Client_command of req_id * IO.op
    | Abort
    | Election_timeout
    | Heartbeat_timeout
    | Snapshots_sent of (rep_id * index) list
    | Snapshot_send_failed of rep_id
    | Readonly_op of ro_op_res Lwt.u

  and 'a cmd_res =
      Redirect of rep_id option
    | Executed of [`OK of 'a | `Error of exn]

  and ro_op_res = OK | Retry

  type gen_result =
      [ `Error of exn
      | `Redirect of rep_id * address
      | `Retry ]

  type 'a cmd_result   = [ gen_result | `OK of 'a ]
  type ro_op_result = [ gen_result | `OK ]

  let get_sent_snapshots t =
    match%lwt Lwt_stream.get t.sent_snapshots with
        None -> fst (Lwt.wait ())
      | Some (peer, last_index) ->
          let l = Lwt_stream.get_available t.sent_snapshots in
            Lwt_stream.njunk (List.length l) t.sent_snapshots>>= fun () ->
            Lwt.return (Snapshots_sent ((peer, last_index) :: l))

  let get_failed_snapshot t =
    match%lwt Lwt_stream.get t.failed_snapshots with
        None -> fst (Lwt.wait ())
      | Some rep_id -> Lwt.return (Snapshot_send_failed rep_id)

  let get_msg t =
    match%lwt Lwt_stream.get t.msg_stream with
      | None -> fst (Lwt.wait ())
      | Some (rep_id, msg) -> Lwt.return (Message (rep_id, msg))

  let get_cmd t =
    match%lwt Lwt_stream.get t.cmd_stream with
      | None -> fst (Lwt.wait ())
      | Some (req_id, op) -> Lwt.return (Client_command (req_id, op))

  let get_ro_op t =
    match%lwt Lwt_stream.get t.ro_op_stream with
      | None -> fst (Lwt.wait ())
      | Some x -> Lwt.return (Readonly_op x)

  let sleep_randomized period =
    Lwt_unix.sleep (period *. 0.75 +. Random.float (period *. 0.5))

  let make
        execute
        ?(election_period = 0.5)
        ?(heartbeat_period = election_period /. 2.) state conn_manager =
    let msg_stream, p     = Lwt_stream.create () in
    let push_msg x        = p (Some x) in
    let cmd_stream, p     = Lwt_stream.create () in
    let push_cmd x        = p (Some x) in
    let ro_op_stream, p   = Lwt_stream.create () in
    let push_ro_op x      = p (Some x) in
    let election_timeout  = match Core.status state with
                              | Follower | Candidate ->
                                  sleep_randomized election_period>>= fun () ->
                                  Lwt.return Election_timeout
                              | Leader -> fst (Lwt.wait ()) in
    let heartbeat         = match Core.status state with
                              | Follower | Candidate -> fst (Lwt.wait ())
                              | Leader ->
                                  Lwt_unix.sleep heartbeat_period>>= fun () ->
                                  Lwt.return Heartbeat_timeout in
    let sent_snapshots, p = Lwt_stream.create () in
    let snapshot_sent x   = p (Some x) in
    let sent_snapshots_th = fst (Lwt.wait ()) in

    let failed_snapshots, p = Lwt_stream.create () in
    let snapshot_failed x   = p (Some x) in
    let failed_snapshot_th  = fst (Lwt.wait ()) in

    let apply_stream, p = Lwt_stream.create () in
    let push_apply x    = p (Some x) in

    let t =
      {
        execute;
        conn_manager;
        heartbeat_period;
        election_period;
        state;
        election_timeout;
        heartbeat;
        sent_snapshots;
        snapshot_sent;
        sent_snapshots_th;
        failed_snapshots;
        snapshot_failed;
        failed_snapshot_th;
        msg_stream;
        push_msg;
        cmd_stream;
        push_cmd;
        ro_op_stream;
        push_ro_op;
        apply_stream;
        push_apply;
        next_req_id   = 42L;
        conns         = RM.empty;
        connecting    = RS.empty;
        running       = true;
        abort         = Lwt.task ();
        get_msg       = fst (Lwt.wait ());
        get_cmd       = fst (Lwt.wait ());
        get_ro_op     = fst (Lwt.wait ());
        pending_ro_ops= Queue.create ();
        pending_cmds  = CMDM.empty;
        leader_signal = Lwt_condition.create ();
        config_change = No_change;
      }
    in
      t.sent_snapshots_th   <- get_sent_snapshots  t;
      t.failed_snapshot_th  <- get_failed_snapshot t;
      t.get_msg             <- get_msg t;
      t.get_cmd             <- get_cmd t;
      t.get_ro_op           <- get_ro_op t;

      ignore begin
        try%lwt
          let rec apply_loop () =
            match%lwt Lwt_stream.get t.apply_stream with
                None -> Lwt.return_unit
              | Some (req_id, op) ->
                  let return_result resp =
                    try%lwt
                      let (_, u), pending = CMDM.extract req_id t.pending_cmds in
                        t.pending_cmds <- pending;
                        Lwt.wakeup_later u (Executed resp);
                        Lwt.return_unit
                    with _ -> Lwt.return_unit
                  in
                    match
                      try (t.execute t op :> [`Sync of _ | `Async of _ | `Error of _])
                      with exn -> `Error exn
                    with
                        `Sync resp ->
                          (try%lwt resp with exn -> Lwt.return (`Error exn)) >>=
                          return_result>>= fun () ->
                          apply_loop ()
                      | `Async resp ->
                          ignore begin
                            (try%lwt resp with exn -> Lwt.return (`Error exn)) >>=
                             return_result
                          end;
                          apply_loop ()
                      | `Error _ as x -> return_result x >>= fun () ->apply_loop ()
          in
            apply_loop ()
        with exn ->
          Logs_lwt.err ~src begin fun m ->
            m "Error in Oraft_lwt apply loop: %a" pp_exn exn
          end
      end;
      t

  let config t = Core.config t.state

  let abort t =
    if not t.running then
      Lwt.return ()
    else begin
      t.running <- false;
      begin try (Lwt.wakeup (snd t.abort) Abort) with _ -> () end;
      RM.bindings t.conns |> List.map snd |> Lwt_list.iter_p IO.abort
    end

  let connect_and_get_msgs t (peer, addr) =
    let rec make_thread = function
        0 -> Lwt_unix.sleep 5. >>= fun () ->make_thread 5
      | n ->
          if RM.mem peer t.conns || RS.mem peer t.connecting ||
             not (List.mem_assoc peer (Core.peers t.state)) then
            Lwt.return ()
          else begin
            t.connecting <- RS.add peer t.connecting;
            match%lwt IO.connect t.conn_manager peer addr with
              | None ->
                  t.connecting <- RS.remove peer t.connecting;
                  Lwt_unix.sleep 0.1 >>= fun () ->make_thread (n - 1)
              | Some conn ->
                  t.connecting <- RS.remove peer t.connecting;
                  t.conns      <- RM.add peer conn t.conns;
                  let rec loop_receive () =
                    match%lwt IO.receive conn with
                        None ->
                          let%lwt () = Lwt_unix.sleep 0.1 in
                            t.conns <- RM.remove peer t.conns;
                            make_thread 5
                      | Some msg ->
                          t.push_msg (peer, msg);
                          loop_receive ()
                  in
                    loop_receive ()
          end
    in
      make_thread 5

  let rec clear_pending_ro_ops t =
    match Queue.Exceptionless.take t.pending_ro_ops with
        None -> ()
      | Some (_, u) -> (try Lwt.wakeup_later u Retry with _ -> ());
                       clear_pending_ro_ops t

  let abort_ongoing_config_change t =
    match t.config_change with
        No_change -> ()
      | New_failover (u, _, _)
      | Remove_failover (u, _)
      | Decommission (u, _)
      | Promote (u, _)
      | Demote (u, _)
      | Replace (u, _, _) ->
          try Lwt.wakeup_later u Retry with _ -> ()

  let notify_config_result t task result =
    t.config_change <- No_change;
    try Lwt.wakeup_later task result with _ -> ()

  let notify_ok_if_mem t u rep_id l =
    notify_config_result t u (if List.mem_assoc rep_id l then OK else Retry)

  let notify_ok_if_not_mem t u rep_id l =
    notify_config_result t u (if List.mem_assoc rep_id l then Retry else OK)

  let check_config_change_completion t =
    match Core.committed_config t.state with
        Joint_config _ -> (* wait for the final Simple_config *) ()
      | Simple_config (active, passive) ->
          match t.config_change with
            | No_change -> ()
            | New_failover (u, rep_id, addr) ->
                if List.mem_assoc rep_id active || List.mem_assoc rep_id passive then
                  notify_config_result t u OK
                else
                  notify_config_result t u Retry
            | Remove_failover (u, rep_id) -> notify_ok_if_not_mem t u rep_id passive
            | Decommission (u, rep_id) ->
                if List.mem_assoc rep_id active || List.mem_assoc rep_id passive then
                  notify_config_result t u Retry
                else
                  notify_config_result t u OK
            | Promote (u, rep_id) -> notify_ok_if_mem t u rep_id active
            | Demote (u, rep_id) -> notify_ok_if_not_mem t u rep_id active
            | Replace (u, replacee, failover) ->
                if not (List.mem_assoc failover active) then
                  notify_config_result t u Retry
                else
                  notify_config_result t u OK

  let rec exec_action t : _ action -> unit Lwt.t = function
    | Reset_election_timeout ->
        t.election_timeout <- (sleep_randomized t.election_period>>= fun () ->
                               Lwt.return Election_timeout);
        Lwt.return ()
    | Reset_heartbeat ->
        t.heartbeat <- (Lwt_unix.sleep t.heartbeat_period>>= fun () ->
                        Lwt.return Heartbeat_timeout);
        Lwt.return ()
    | Become_candidate
    | Become_follower None as ev ->
        clear_pending_ro_ops t;
        abort_ongoing_config_change t;
        t.heartbeat <- fst (Lwt.wait ());
        Logs_lwt.info ~src begin fun m ->
          m "Becoming %s"
            (match ev with
              | Become_candidate -> "candidate"
              | _ -> "follower (unknown leader)")
        end >>= fun () ->
        exec_action t Reset_election_timeout
    | Become_follower (Some id) ->
        clear_pending_ro_ops t;
        abort_ongoing_config_change t;
        Lwt_condition.broadcast t.leader_signal ();
        t.heartbeat <- fst (Lwt.wait ());
        Logs_lwt.info ~src
          (fun m -> m "Becoming follower leader:%S" id) >>= fun () ->
        exec_action t Reset_election_timeout
    | Become_leader ->
        clear_pending_ro_ops t;
        abort_ongoing_config_change t;
        Lwt_condition.broadcast t.leader_signal ();
        Logs_lwt.info ~src (fun m -> m "Becoming leader") >>= fun () ->
        exec_action t Reset_election_timeout>>= fun () ->
        exec_action t Reset_heartbeat
    | Changed_config ->
        check_config_change_completion t;
        Lwt.return_unit
    | Apply l ->
        List.iter (fun (index, (req_id, op), term) -> t.push_apply (req_id, op)) l;
        Lwt.return_unit
    | Redirect (rep_id, (req_id, _)) -> begin
        try%lwt
          let (_, u), pending_cmds = CMDM.extract req_id t.pending_cmds in
            t.pending_cmds <- pending_cmds;
            Lwt.wakeup_later u (Redirect rep_id);
            Lwt.return ()
        with _ -> Lwt.return ()
      end
    | Send (rep_id, addr, msg) ->
        (* we allow to run this in parallel with rest RAFT algorithm.
         * It's OK to reorder sends. *)
        (* TODO: limit the number of msgs in outboung queue.
         * Drop after the nth? *)
        ignore begin try%lwt
          let c = RM.find rep_id t.conns in
            if IO.is_saturated c then Lwt.return_unit
            else IO.send c msg
        with _ ->
          (* cannot send -- partition *)
          Lwt.return ()
        end;
        Lwt.return ()
    | Send_snapshot (rep_id, addr, idx, config) ->
        ignore begin
          match%lwt IO.prepare_snapshot (RM.find rep_id t.conns) idx config with
            | None -> Lwt.return ()
            | Some transfer ->
                try%lwt
                  match%lwt IO.send_snapshot transfer with
                      true -> t.snapshot_sent (rep_id, idx);
                              Lwt.return ()
                    | false -> failwith "error"
                with _ ->
                  t.snapshot_failed rep_id;
                  Lwt.return ()
        end;
        Lwt.return ()
    | Stop -> Lwt.fail Stop_node
    | Exec_readonly n ->
        (* can execute all RO ops whose ID is >= n *)
        let rec notify_ok () =
          match Queue.Exceptionless.peek t.pending_ro_ops with
              None -> Lwt.return_unit
            | Some (m, _) when m > n -> Lwt.return_unit
            | Some (_, u) ->
                ignore (Queue.Exceptionless.take t.pending_ro_ops);
                Lwt.wakeup_later u OK;
                notify_ok ()
        in
          notify_ok ()

  let exec_actions t l = Lwt_list.iter_s (exec_action t) l

  let rec run t =
    if not t.running then Lwt.return ()
    else begin
      (* Launch new connections as needed.
       * connect_and_get_msgs will ignore peers for which a connection already
       * exists or is being established. *)
      ignore (List.map (connect_and_get_msgs t) (Core.peers t.state));
      match%lwt
        Lwt.choose
          [ t.election_timeout;
            fst t.abort;
            t.get_msg;
            t.get_cmd;
            t.get_ro_op;
            t.heartbeat;
            t.sent_snapshots_th;
          ]
      with
        | Abort -> t.running <- false; Lwt.return ()
        | Readonly_op u -> begin
            t.get_ro_op <- get_ro_op t;
            match Core.readonly_operation t.state with
                (s, None) -> Lwt.wakeup_later u Retry;
                             Lwt.return_unit
              | (s, Some (id, actions)) ->
                  Queue.push (id, u) t.pending_ro_ops;
                  t.state <- s;
                  exec_actions t actions>>= fun () ->
                  run t
          end
        | Client_command (req_id, op) ->
            let state, actions = Core.client_command (req_id, op) t.state in
              t.get_cmd <- get_cmd t;
              t.state   <- state;
              exec_actions t actions>>= fun () ->
              run t
        | Message (rep_id, msg) ->
            let state, actions = Core.receive_msg t.state rep_id msg in
              t.get_msg <- get_msg t;
              t.state <- state;
              exec_actions t actions>>= fun () ->
              run t
        | Election_timeout ->
            let state, actions = Core.election_timeout t.state in
              t.state <- state;
              exec_actions t actions>>= fun () ->
              run t
        | Heartbeat_timeout ->
            let state, actions = Core.heartbeat_timeout t.state in
              t.state <- state;
              exec_actions t actions>>= fun () ->
              run t
        | Snapshots_sent data ->
            let state, actions =
              List.fold_left
                (fun (s, actions) (peer, last_index) ->
                   let s, actions' = Core.snapshot_sent peer ~last_index s in
                     (s, actions' @ actions))
                (t.state, [])
                data
            in
              t.sent_snapshots_th <- get_sent_snapshots t;
              t.state <- state;
              exec_actions t actions>>= fun () ->
              run t
        | Snapshot_send_failed rep_id ->
            let state, actions = Core.snapshot_send_failed rep_id t.state in
              t.failed_snapshot_th <- get_failed_snapshot t;
              t.state <- state;
              exec_actions t actions>>= fun () ->
              run t
    end

  let run t =
    try%lwt
      run t
    with Stop_node -> t.running <- false; Lwt.return ()

  let gen_req_id t =
    let id = t.next_req_id in
      t.next_req_id <- Int64.succ id;
      (Core.id t.state, id)

  let rec exec_aux t f =
    match Core.status t.state, Core.leader_id t.state with
      | Follower, Some leader_id -> begin
          match List.Exceptionless.assoc leader_id (Core.peers t.state) with
              Some address -> Lwt.return (`Redirect (leader_id, address))
            | None ->
                (* redirect to a random server, hoping it knows better *)
                try%lwt
                  let leader_id, address =
                    Core.peers t.state |>
                    Array.of_list |>
                    (fun x -> if x = [||] then failwith "empty"; x) |>
                    (fun a -> a.(Random.int (Array.length a)))
                  in
                    Lwt.return (`Redirect (leader_id, address))
                with _ ->
                  Lwt_unix.sleep retry_delay>>= fun () ->
                  Lwt.return `Retry
        end
      | Candidate, _ | Follower, _ ->
          (* await leader, retry *)
          Lwt_condition.wait t.leader_signal>>= fun () ->
          exec_aux t f
      | Leader, _ ->
          f t

  let rec execute t cmd =
    exec_aux t
      (fun t ->
          let req_id = gen_req_id t in
          let task   = Lwt.task () in
            t.pending_cmds <- CMDM.add req_id task t.pending_cmds;
            t.push_cmd (req_id, cmd);
            match%lwt fst task with
                Executed res -> Lwt.return (res :> _ cmd_result)
              | Redirect _ -> execute t cmd)

  let rec readonly_operation t =
    if Core.is_single_node_cluster t.state then
      Lwt.return `OK
    else
      exec_aux t
        (fun t ->
           let th, u = Lwt.task () in
             t.push_ro_op u;
             match%lwt th with
                 OK -> Lwt.return `OK
               | Retry -> readonly_operation t)

  let compact_log t index =
    t.state <- Core.compact_log index t.state

  module Config =
  struct
    type result =
      [
      | `OK
      | `Redirect of rep_id * address
      | `Retry
      | `Cannot_change
      | `Unsafe_change of simple_config * passive_peers
      ] [@@deriving bin_io]

    let get t = Core.config t.state

    let rec perform_change t perform mk_change : result Lwt.t =
      match t.config_change with
          New_failover _ | Remove_failover _ | Decommission _
        | Promote _ | Demote _ | Replace _ ->
            Lwt_unix.sleep retry_delay>>= fun () ->
            perform_change t perform mk_change
        | No_change ->
            match perform t.state with
                `Already_changed -> Lwt.return `OK
              | `Cannot_change | `Unsafe_change _ as x -> Lwt.return x
              | `Redirect (Some x) -> Lwt.return (`Redirect x)
              | `Redirect None -> Lwt.return `Retry
              | `Change_in_process ->
                  Lwt_unix.sleep retry_delay>>= fun () ->
                  perform_change t perform mk_change
              | `Start_change state ->
                  t.state <- state;
                  let th, u = Lwt.task () in
                    t.config_change <- mk_change u;
                    match%lwt th with
                        OK -> Lwt.return `OK
                      | Retry ->
                          Lwt_unix.sleep retry_delay>>= fun () ->
                          perform_change t perform mk_change

    let rec add_failover t rep_id addr =
      perform_change t
        (Core.Config.add_failover rep_id addr)
        (fun u -> New_failover (u, rep_id, addr))

    let remove_failover t rep_id =
      perform_change t
        (Core.Config.remove_failover rep_id)
        (fun u -> Remove_failover (u, rep_id))

    let decommission t rep_id =
      perform_change t
        (Core.Config.decommission rep_id)
        (fun u -> Decommission (u, rep_id))

    let promote t rep_id =
      perform_change t
        (Core.Config.promote rep_id)
        (fun u -> Promote (u, rep_id))

    let demote t rep_id =
      perform_change t
        (Core.Config.demote rep_id)
        (fun u -> Demote (u, rep_id))

    let replace t ~replacee ~failover =
      perform_change t
        (Core.Config.replace ~replacee ~failover)
        (fun u -> Replace (u, replacee, failover))
  end
end

module type SERVER_CONF =
sig
  type op [@@deriving sexp,bin_io]
  val string_of_op : op -> string
  val op_of_string : string -> op
  val node_sockaddr : address -> Unix.sockaddr
  val string_of_address : address -> string
end

type 'a conn_wrapper =
    {
      wrap_incoming_conn :
        Lwt_unix.file_descr -> (Lwt_io.input_channel * Lwt_io.output_channel) Lwt.t;
      wrap_outgoing_conn :
        Lwt_unix.file_descr ->
        (Lwt_io.input_channel * Lwt_io.output_channel) Lwt.t;
    }

type simple_wrapper =
  Lwt_unix.file_descr -> (Lwt_io.input_channel * Lwt_io.output_channel) Lwt.t

let make_client_conn_wrapper f =
  { wrap_incoming_conn =
      (fun fd -> Lwt.fail_with "Incoming conn wrapper invoked in client");
    wrap_outgoing_conn = f;
  }

let make_server_conn_wrapper ~incoming ~outgoing =
  { wrap_incoming_conn = incoming; wrap_outgoing_conn = outgoing }

let trivial_wrap_outgoing_conn ?buffer_size fd =
  let close =
    lazy begin
      (try%lwt
        Lwt_unix.shutdown fd Unix.SHUTDOWN_ALL;
        Lwt.return_unit
      with Unix.Unix_error(Unix.ENOTCONN, _, _) ->
        (* This may happen if the server closed the connection before us *)
        Lwt.return_unit)
        [%finally
          Lwt_unix.close fd]
    end in
  let buf1 = BatOption.map Lwt_bytes.create buffer_size in
  let buf2 = BatOption.map Lwt_bytes.create buffer_size in
    try%lwt
      (try Lwt_unix.set_close_on_exec fd with Invalid_argument _ -> ());
      Lwt.return (Lwt_io.make ?buffer:buf1
                ~close:(fun _ -> Lazy.force close)
                ~mode:Lwt_io.input (Lwt_bytes.read fd),
              Lwt_io.make ?buffer:buf2
                ~close:(fun _ -> Lazy.force close)
                ~mode:Lwt_io.output (Lwt_bytes.write fd))
    with exn ->
      let%lwt () = Lwt_unix.close fd in
      Lwt.fail exn

let trivial_wrap_incoming_conn ?buffer_size fd =
  let buf1 = BatOption.map Lwt_bytes.create buffer_size in
  let buf2 = BatOption.map Lwt_bytes.create buffer_size in
    Lwt.return
      (Lwt_io.of_fd ?buffer:buf1 ~mode:Lwt_io.input fd,
       Lwt_io.of_fd ?buffer:buf2 ~mode:Lwt_io.output fd)

let trivial_conn_wrapper ?buffer_size () =
  { wrap_incoming_conn = trivial_wrap_incoming_conn ?buffer_size;
    wrap_outgoing_conn = trivial_wrap_outgoing_conn ?buffer_size;
  }

let wrap_outgoing_conn w fd = w.wrap_outgoing_conn fd
let wrap_incoming_conn w fd = w.wrap_incoming_conn fd

module Simple_IO(C : SERVER_CONF) =
struct

  type op = C.op
  type msg = req_id * C.op [@@deriving sexp,bin_io]

  module M  = Map.Make(String)

  let src = Logs.Src.create "oraft_lwt.io"

  type conn_manager =
      {
        id            : string;
        sock          : Lwt_unix.file_descr;
        mutable conns : connection M.t;
        conn_signal   : unit Lwt_condition.t;
        conn_wrapper  : [`Incoming | `Outgoing] conn_wrapper;
      }

  and connection =
    {
      id             : rep_id;
      mgr            : conn_manager;
      ich            : Lwt_io.input_channel;
      och            : Lwt_io.output_channel;
      mutable closed : bool;
      mutable in_buf : Lwt_bytes.t;
      out_buf        : Lwt_bytes.t;
      mutable noutgoing : int;
    }

  let make ?(conn_wrapper = trivial_conn_wrapper ()) ~id addr =
    let sock = Lwt_unix.(socket (Unix.domain_of_sockaddr addr) Unix.SOCK_STREAM 0) in
      Lwt_unix.setsockopt sock Unix.SO_REUSEADDR true;
      Lwt_unix.bind sock addr >>= fun () ->
      Lwt_unix.listen sock 256;

      let rec accept_loop t =
        let%lwt (fd, _addr) = Lwt_unix.accept sock in
          ignore begin try%lwt
              (* the following are not supported for ADDR_UNIX sockets, so catch
               * possible exceptions *)
              (try Lwt_unix.setsockopt fd Unix.TCP_NODELAY true with _ -> ());
              (try Lwt_unix.setsockopt fd Unix.SO_KEEPALIVE true with _ -> ());
              let%lwt ich, och = conn_wrapper.wrap_incoming_conn fd in
              let%lwt id       = Lwt_io.read_line ich in
              let c        = { id; mgr = t; ich; och; closed = false;
                               in_buf = Lwt_bytes.create 4096;
                               out_buf = Lwt_bytes.create 4096;
                               noutgoing = 0;
                             }
              in
                t.conns <- M.add id c t.conns;
                Lwt_condition.broadcast t.conn_signal ();
                Logs_lwt.info ~src (fun m -> m "Incoming connection from peer %S" id)
            with _ ->
              Lwt_unix.shutdown fd Unix.SHUTDOWN_ALL;
              Lwt_unix.close fd
          end;
          accept_loop t
      in
      let conn_signal = Lwt_condition.create () in
      let t           = { id; sock; conn_signal; conns = M.empty; conn_wrapper; } in
        ignore begin
          try%lwt
            Logs_lwt.info ~src
              (fun m -> m "Running node server at %a" pp_saddr addr) >>= fun () ->
            accept_loop t
          with
            | Exit -> Lwt.return ()
            | exn -> Logs_lwt.err ~src begin fun m ->
                m "Error in connection manager accept loop: %a" pp_exn exn
              end
        end;
        Lwt.return t

  let connect t dst_id addr =
    match M.Exceptionless.find dst_id t.conns with
        Some _ as x -> Lwt.return x
      | None when dst_id < t.id -> (* wait for other end to connect *)
          let rec await_conn () =
            match M.Exceptionless.find dst_id t.conns with
                Some _ as x -> Lwt.return x
              | None -> Lwt_condition.wait t.conn_signal>>= fun () ->
                       await_conn ()
          in
            await_conn ()
      | None -> (* we must connect ourselves *)
          try%lwt
            Logs_lwt.info ~src begin fun m ->
              m "Connecting to %S" (C.string_of_address addr)
            end >>= fun () ->
            let saddr    = C.node_sockaddr addr in
            let fd       = Lwt_unix.socket (Unix.domain_of_sockaddr saddr)
                             Unix.SOCK_STREAM 0 in
            let%lwt ()       = Lwt_unix.connect fd saddr in
            let%lwt ich, och = t.conn_wrapper.wrap_outgoing_conn fd in
              try%lwt
                (try Lwt_unix.setsockopt fd Unix.TCP_NODELAY true with _ -> ());
                (try Lwt_unix.setsockopt fd Unix.SO_KEEPALIVE true with _ -> ());
                Lwt_io.write och (t.id ^ "\n")>>= fun () ->
                Lwt_io.flush och>>= fun () ->
                Lwt.return (Some { id = dst_id; mgr = t; ich; och; closed = false;
                                   in_buf = Lwt_bytes.create 4096;
                                   out_buf = Lwt_bytes.create 4096;
                               noutgoing = 0; })
              with exn ->
                Lwt_unix.close fd>>= fun () ->
                Lwt.fail exn
          with _ -> Lwt.return_none

  let is_saturated conn = conn.noutgoing > 10

  open Oraft

  let abort c =
    if c.closed then Lwt.return ()
    else begin
      c.mgr.conns <- M.remove c.id c.mgr.conns;
      c.closed    <- true;
      Lwt_io.abort c.och
    end

  let send c msg =
    if c.closed then Lwt.return ()
    else begin
      Logs_lwt.debug ~src begin fun m ->
        m "Sending %a" Sexplib.Sexp.pp_hum (sexp_of_message sexp_of_msg msg)
      end >>= fun () ->
        (try%lwt
          c.noutgoing <- c.noutgoing + 1;
          Lwt_io.atomic begin fun och ->
            let msglen = bin_write_message bin_write_msg c.out_buf ~pos:0 msg in
              Lwt_io.LE.write_int och msglen >>= fun () ->
              Lwt_io.direct_access och (da_write_msg c.out_buf 0 msglen)
          end c.och
         with exn ->
           let%lwt () = Logs_lwt.info ~src begin fun m ->
               m "Error on send to %s, closing connection. %a" c.id pp_exn exn
             end
          in
            abort c)
          [%finally
            c.noutgoing <- c.noutgoing - 1;
            Lwt.return_unit]
    end

  let receive c =
    if c.closed then
      Lwt.return None
    else
      try%lwt
        Lwt_io.atomic begin fun ich ->
          Lwt_io.LE.read_int ich >>= fun len ->
          Lwt_io.direct_access ich (da_read_msg c.in_buf 0 len) >>= fun () ->
          let msg = bin_read_message bin_read_msg c.in_buf ~pos_ref:(ref 0) in
            Logs_lwt.debug ~src begin fun m ->
              m "Received %a"
                Sexplib.Sexp.pp_hum (sexp_of_message sexp_of_msg msg)
            end >>= fun () ->
            Lwt.return_some msg
        end c.ich
      with exn ->
        Logs_lwt.info ~src begin fun m ->
          m "Error on receive from %S, closing connection. %a" c.id pp_exn exn
        end >>= fun () ->
        abort c >>= fun () ->
        Lwt.return None

  type snapshot_transfer = unit

  let prepare_snapshot conn index config = Lwt.return None
  let send_snapshot () = Lwt.return false
end

module Simple_server(C : SERVER_CONF) =
struct
  module IO = Simple_IO(C)
  include Make_server(IO)

  let make_conn_manager = IO.make
end
