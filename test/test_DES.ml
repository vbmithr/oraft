open Printf

module List   = BatList
module Map    = BatMap
module Int64  = BatInt64
module Option = BatOption
module Array  = BatArray
module RND    = Random.State
module C      = Oraft.Core

module CLOCK =
struct
  include BatInt64
  type delta = t
end

module type EVENT_QUEUE =
sig
  open Oraft.Types
  type 'a t

  val create     : unit -> 'a t
  val schedule   : 'a t -> CLOCK.delta -> rep_id -> 'a -> CLOCK.t
  val next       : 'a t -> (CLOCK.t * rep_id * 'a) option
  val is_empty   : 'a t -> bool
end

module DES :
sig
  open Oraft.Types

  module Event_queue : EVENT_QUEUE

  type ('op, 'snapshot) event =
    | Election_timeout
    | Heartbeat_timeout
    | Command of 'op
    | Message of rep_id * 'op message
    | Func of (CLOCK.t -> unit)
    | Install_snapshot of rep_id * 'snapshot * term * index * config
    | Snapshot_sent of rep_id * index
    | Snapshot_send_failed of rep_id

  type ('op, 'app_state, 'snapshot) t
  type ('op, 'app_state) node

  val make :
    ?rng:RND.t ->
    ?ev_queue:('op, 'snapshot) event Event_queue.t ->
    num_nodes:int ->
    election_period:CLOCK.t ->
    heartbeat_period:CLOCK.t ->
    rtt:CLOCK.t ->
    (unit -> 'app_state) -> ('op, 'app_state, 'snapshot) t

  val random_node_id : (_, _, _) t -> rep_id
  val live_nodes   : ('op, 'app_state, _) t -> ('op, 'app_state) node list

  val node_id       : (_, _) node -> rep_id
  val app_state     : (_, 'app_state) node -> 'app_state
  val set_app_state : (_, 'app_state) node -> 'app_state -> unit
  val leader_id     : (_, _) node -> rep_id option

  val simulate :
    ?verbose:bool ->
    ?string_of_cmd:('op -> string) ->
    msg_loss_rate:float ->
    on_apply:(time:Int64.t -> ('op, 'app_state) node ->
              (index * 'op * term) list ->
              [`Snapshot of index * 'app_state |
               `State of 'app_state]) ->
    take_snapshot:(('op, 'app_state) node -> index * 'snapshot * term) ->
    install_snapshot:(('op, 'app_state) node -> 'snapshot -> unit) ->
    ('op, 'app_state, 'snapshot) t -> int
end =
struct
  open Oraft.Types

  module Event_queue =
  struct
    type rep_id = string
    module type PACK =
    sig
      type elm
      module M : BatHeap.H with type elem = elm
      val h : M.t ref
      val t : CLOCK.t ref
    end

    type 'a m = (module PACK with type elm = 'a)
    type 'a t = (Int64.t * rep_id * 'a) m

    let create (type a) () : a t =
      let module P =
        struct
          type elm = Int64.t * rep_id * a
          module M = BatHeap.Make(struct
                                    type t = elm
                                    let compare (c1, _, _) (c2, _, _) =
                                      Int64.compare c1 c2
                                  end)
          let h = ref M.empty
          let t = ref 0L
        end
      in
        (module P)

    let schedule (type a) ((module P) : a t) dt node (ev : a) =
      let t = CLOCK.(!P.t + dt) in
        P.h := P.M.add (t, node, ev) !P.h;
        t

    let is_empty (type a) ((module P) : a t) = P.M.size !P.h = 0

    let next (type a) ((module P) : a t) =
      try
        let (t, _, _) as x = P.M.find_min !P.h in
          P.h := P.M.del_min !P.h;
          P.t := t;
          Some x
      with Invalid_argument _ -> None
  end

  module NODES =
  struct
    module M = Map.Make(String)
    type 'a t = { mutable ids : rep_id array; mutable m : 'a M.t }

    let create () = { ids = [||]; m = M.empty }

    let find t k  =
      try Some (M.find k t.m) with Not_found -> None

    let iter f t  = M.iter f t.m

    let random t rng = M.find t.ids.(RND.int rng (Array.length t.ids)) t.m

    let add t k v =
      if not (M.mem k t.m) then begin
        t.ids <- Array.append [|k|] t.ids;
        t.m   <- M.add k v t.m
      end

    let remove t k =
      t.ids <- Array.filter ((<>) k) t.ids;
      t.m   <- M.remove k t.m
  end

  type ('op, 'snapshot) event =
    | Election_timeout
    | Heartbeat_timeout
    | Command of 'op
    | Message of rep_id * 'op message
    | Func of (CLOCK.t -> unit)
    | Install_snapshot of rep_id * 'snapshot * term * index * config
    | Snapshot_sent of rep_id * index
    | Snapshot_send_failed of rep_id

  type ('op, 'app_state) node =
      {
        id                     : rep_id;
        addr                   : address;
        mutable state          : 'op C.state;
        mutable next_heartbeat : CLOCK.t option;
        mutable next_election  : CLOCK.t option;
        mutable app_state      : 'app_state;
        mutable stopped        : bool;
      }

  type ('op, 'app_state, 'snapshot) t =
      {
        rng              : RND.t;
        ev_queue         : ('op, 'snapshot) event Event_queue.t;
        nodes            : ('op, 'app_state) node NODES.t;
        election_period  : CLOCK.t;
        heartbeat_period : CLOCK.t;
        rtt              : CLOCK.t;
        make_node        : unit -> ('op, 'app_state) node;
      }

  let mk_node mk_app_state config (id, addr) =
    let state     = C.make
                      ~id ~current_term:0L ~voted_for:None
                      ~log:[] ~config () in
    let app_state = mk_app_state () in
      { id; addr; state; app_state;
        stopped = false; next_heartbeat = None; next_election = None;
      }

  let node_id n         = n.id
  let leader_id n       = C.leader_id n.state
  let app_state n       = n.app_state
  let set_app_state n x = n.app_state <- x

  let get_leader (type op) (type app_state) t =
    let module M = struct exception N of (op, app_state) node end in
      try
        NODES.iter
          (fun _ node -> if C.status node.state = Leader then raise (M.N node))
          t.nodes;
        (* we pick an arbitrary config *)
        NODES.random t.nodes t.rng
      with M.N n -> n

  let get_leader_conf t =
    C.committed_config (get_leader t).state

  let make
        ?(rng = RND.make_self_init ())
        ?(ev_queue = Event_queue.create ())
        ~num_nodes
        ~election_period ~heartbeat_period ~rtt
        mk_app_state =
    let active   = List.init num_nodes
                     (fun n -> let s = sprintf "n%03d" n in (s, s)) in
    let passive  = [] in
    let config   = Simple_config (active, passive) in

    let mk_nodes l = List.map (mk_node mk_app_state config) l |> Array.of_list in

    let next_node_id =
      let n = ref (num_nodes - 1) in
        (fun () -> incr n; sprintf "n%03d" !n) in

    let active   = mk_nodes active in
    let passive  = mk_nodes passive in

    let nodes    = NODES.create () in
    let ()       = Array.(iter (fun n -> NODES.add nodes n.id n)
                            (append active passive)) in

    let rec make_node () =
      let id     = next_node_id () in
      let addr   = "proto://" ^ id in
      let config = get_leader_conf t in
        mk_node mk_app_state config (id, addr)

    and t =
      { rng; ev_queue; election_period; heartbeat_period; rtt;
        nodes; make_node;
      }
    in
      t

  let random_node_id t = (NODES.random t.nodes t.rng).id

  let live_nodes t =
    let l = ref [] in
      NODES.iter (fun _ n -> if not n.stopped then l := n :: !l) t.nodes;
      !l

  let s_of_simple_config l =
    List.map (fun (id, addr) -> sprintf "%S:%S" id addr) l |> String.concat "; "

  let string_of_config c =
      match c with
          Simple_config (c, passive) ->
            sprintf "Simple ([%s], [%s])"
              (s_of_simple_config c) (s_of_simple_config passive)
        | Joint_config (c1, c2, passive) ->
            sprintf "Joint ([%s], [%s], [%s])"
              (s_of_simple_config c1) (s_of_simple_config c2)
              (s_of_simple_config passive)

  let string_of_msg string_of_cmd = function
      Request_vote { term; candidate_id; last_log_term; last_log_index; _ } ->
        sprintf "Request_vote %S last_term:%Ld last_index:%Ld @ %Ld"
          candidate_id last_log_term last_log_index term
    | Vote_result { term; vote_granted } ->
        sprintf "Vote_result %b @ %Ld" vote_granted term
    | Append_entries { term; prev_log_index; prev_log_term; entries; _ } ->
        let string_of_entry = function
            Nop -> "Nop"
          | Op cmd -> "Op " ^ Option.default (fun _ -> "<cmd>") string_of_cmd cmd
          | Config c -> "Config [" ^ string_of_config c ^ "]" in

        let payload_desc =
          entries |>
          List.map
            (fun (index, (entry, term)) ->
               sprintf "(%Ld, %s, %Ld)" index
                 (string_of_entry entry) term) |>
          String.concat ", "
        in
          sprintf "Append_entries (%Ld, %Ld, [%s]) @ %Ld"
            prev_log_index prev_log_term payload_desc term
    | Append_result { term; result = Append_success last_log_index } ->
        sprintf "Append_result success %Ld @ %Ld" last_log_index term
    | Append_result { term; result = Append_failure prev_log_index } ->
        sprintf "Append_result failure %Ld @ %Ld" prev_log_index term
    | Pong { term; n } -> sprintf "Pong %Ld @ %Ld" n term
    | Ping { term; n } -> sprintf "Ping %Ld @ %Ld" n term

  let describe_event string_of_cmd = function
      Election_timeout -> "Election_timeout"
    | Heartbeat_timeout -> "Heartbeat_timeout"
    | Command cmd -> sprintf "Command %s"
                       (Option.default  (fun _ -> "<cmd>") string_of_cmd cmd)
    | Message (rep_id, msg) ->
        sprintf "Message (%S, %s)" rep_id (string_of_msg string_of_cmd msg)
    | Func _ -> "Func _"
    | Install_snapshot (src, _, term, index, _config) ->
        sprintf "Install_snapshot (%S, _, %Ld, %Ld, _)" src term index
    | Snapshot_sent (dst, idx) -> sprintf "Snapshot_sent %S last_index:%Ld" dst idx
    | Snapshot_send_failed dst -> sprintf "Snapshot_send_failed %S" dst

  let schedule_election t node =
    let dt = CLOCK.(t.election_period - t.election_period / 4L +
                    of_int (RND.int t.rng (to_int t.election_period lsr 2))) in
    let t1 = Event_queue.schedule t.ev_queue dt node.id Election_timeout in
      node.next_election <- Some t1

  let schedule_heartbeat t node =
    let t1 = Event_queue.schedule
               t.ev_queue t.heartbeat_period node.id Heartbeat_timeout
    in
      node.next_heartbeat <- Some t1

  let unschedule_election _t node =
    node.next_election <- None

  let unschedule_heartbeat _t node =
    node.next_heartbeat <- None

  let send_cmd t ?(dt=200L) node_id cmd =
    ignore (Event_queue.schedule t.ev_queue dt node_id (Command cmd))

  let must_account time node = function
      Election_timeout -> begin match node.next_election with
          Some t when t = time -> true
        | _ -> false
      end
    | Heartbeat_timeout -> begin match node.next_heartbeat with
          Some t when t = time -> true
        | _ -> false
      end
    | Func _ -> false
    | Command _ | Message _ | Install_snapshot _
    | Snapshot_sent _ | Snapshot_send_failed _ -> true

  module CONFIG_MANAGER :
  sig
    type config_manager

    val make :
      ?verbose:bool -> period:int ->
      ('op, 'app_state, 'snapshot) t -> config_manager

    val tick : config_manager -> int -> unit
  end =
  struct
    type config_manager =
        { verbose : bool; mutable ticks : int;
          period : int; des : des;
        }

    and des   = DES : (_, _, _) t -> des

    let make ?(verbose=false) ~period des =
      { verbose; ticks = 0; period; des = DES des; }

    let add_or_promote_failover t des =

      match get_leader_conf des with
        | Simple_config (_, []) -> (* add failover *)
            begin
              let newnode = des.make_node () in
              let leader  = get_leader des in
              let addr    = "proto://" ^ newnode.id in

                match C.Config.add_failover newnode.id addr leader.state with
                    `Already_changed | `Change_in_process
                  | `Redirect _ | `Unsafe_change _ | `Cannot_change -> ()
                  | `Start_change state ->
                      if t.verbose then
                        printf
                          "!! Adding passive node %S, transitioning to %s\n"
                          newnode.id
                          (string_of_config (C.config state));
                      leader.state <- state;
                      NODES.add des.nodes newnode.id newnode
            end

        | Joint_config _ | Simple_config ([], _) -> ()

        | Simple_config ((_ :: _ as active), (_ :: _ as passive)) ->
            let replacee = List.sort compare active |> List.hd |> fst in
            let failover = List.sort compare passive |> List.hd |> fst in

            let leader   = get_leader des in

              match C.Config.replace ~replacee ~failover leader.state with
                  `Already_changed | `Change_in_process
                | `Redirect _ | `Unsafe_change _ | `Cannot_change -> ()

                | `Start_change state ->
                    if t.verbose then
                      printf "!! Replacing node %S with %S\n" replacee failover;
                    leader.state <- state

    let tick ({ des = DES des; _ } as t) n =
      t.ticks <- t.ticks + n;
      (* printf "TICK %d\n" n; *)
      if t.ticks > t.period then begin
        t.ticks <- 0;
        add_or_promote_failover t des
      end
  end

  module FAILURE_SIMULATOR :
  sig
    type t

    val make : ?verbose:bool -> msg_loss_rate:float -> period:int -> RND.t -> t
    val tick : t -> rep_id list -> int -> unit
    val is_msg_lost : t -> src:rep_id -> dst:rep_id -> bool
  end =
  struct
    type t =
        {
          period                      : int;
          mutable ticks_to_transition : int;
          mutable state               : [`Down of rep_id | `Up ];
          msg_loss_rate               : float;
          rng                         : RND.t;
          verbose                     : bool;
        }

    let make ?(verbose=false) ~msg_loss_rate ~period rng =
      { verbose; period; ticks_to_transition = period; msg_loss_rate; rng; state = `Up }

    let tick t node_ids n =
      t.ticks_to_transition <- t.ticks_to_transition - n;
      if t.ticks_to_transition <= 0 then begin
        t.ticks_to_transition <- t.period;
        match t.state with
            `Down id ->
              if t.verbose then printf "### Node %S BACK\n" id;
              t.state <- `Up
          | `Up ->
              let node_ids = Array.of_list node_ids in
              let id       = node_ids.(RND.int t.rng (Array.length node_ids)) in
                if t.verbose then printf "### Node %S PARTITIONED\n" id;
                t.state <- `Down id
      end

    let is_msg_lost t ~src ~dst = match t.state with
      | `Down peer when peer = dst || peer = src -> true
      | _ -> RND.float t.rng 1.0 < t.msg_loss_rate
  end

  let simulate
        ?(verbose = false) ?string_of_cmd ~msg_loss_rate
        ~on_apply ~take_snapshot ~install_snapshot t =

    let send_cmd ?(dst = random_node_id t) ?dt cmd =
      send_cmd ?dt t dst cmd in

    let fail_sim  = FAILURE_SIMULATOR.make
                      ~verbose ~msg_loss_rate ~period:1000 t.rng in
    let configmgr = CONFIG_MANAGER.make ~verbose t ~period:5000 in

    let react_to_event time node ev =
      (* Tick at least once per event so that fallen nodes eventually come
       * back even if the cluster is making no progress (i.e. in absence of
       * commits by other nodes). Avoids getting stuck when commits are
       * blocked because there's no safe quorum (only nodes that will not be
       * decommissioned) during a configuration change. *)
      FAILURE_SIMULATOR.tick fail_sim
        (node.id :: (C.peers node.state |> List.map fst)) 1;
      CONFIG_MANAGER.tick configmgr 1;
      let considered = must_account time node ev in
      let () =
        if considered && verbose then
          printf "%Ld @ %s -> %s\n" time node.id (describe_event string_of_cmd ev) in

      let s, actions = match ev with
          Election_timeout -> begin
            match node.next_election with
                Some t when t = time -> C.election_timeout node.state
              | _ -> (node.state, [])
          end
        | Heartbeat_timeout -> begin
            match node.next_heartbeat with
              | Some t when t = time -> C.heartbeat_timeout node.state
              | _ -> (node.state, [])
          end
        | Command c -> C.client_command c node.state
        | Message (peer, msg) -> C.receive_msg node.state peer msg
        | Func f ->
            f time;
            (node.state, [])
        | Install_snapshot (src, snapshot, last_term, last_index, config) ->
            let s, accepted = C.install_snapshot
                                ~last_term ~last_index ~config node.state
            in
              ignore (Event_queue.schedule t.ev_queue 40L src
                        (Snapshot_sent (node.id, last_index)));
              if accepted then install_snapshot node snapshot;
              (s, [])
        | Snapshot_sent (peer, last_index) ->
            C.snapshot_sent peer ~last_index node.state
        | Snapshot_send_failed peer ->
            C.snapshot_send_failed peer node.state
      in

      let rec exec_action = function
          Apply cmds ->
            if verbose then
              printf " Apply %d cmds [%s]\n"
                (List.length cmds)
                (List.map
                   (fun (idx, cmd, term) ->
                      sprintf "(%Ld, %s, %Ld)"
                        idx
                        (Option.default (fun _ -> "<cmd>") string_of_cmd cmd)
                        term)
                   cmds |>
                 String.concat ", ");
            (* simulate current leader being cached by client *)
            begin match on_apply ~time node cmds with
                `Snapshot (last_index, app_state) ->
                  node.app_state <- app_state;
                  node.state <- C.compact_log last_index node.state
              | `State app_state ->
                  node.app_state <- app_state
            end
        | Become_candidate ->
            if verbose then printf " Become_candidate\n";
            unschedule_heartbeat t node;
            exec_action Reset_election_timeout
        | Become_follower None ->
            if verbose then printf " Become_follower\n";
            unschedule_heartbeat t node;
            exec_action Reset_election_timeout
        | Become_follower (Some leader) ->
            if verbose then printf " Become_follower %S\n" leader;
            unschedule_heartbeat t node;
            exec_action Reset_election_timeout
        | Become_leader ->
            if verbose then printf " Become_leader\n";
            unschedule_election t node;
            schedule_election t node;
            schedule_heartbeat t node
        | Changed_config ->
            if verbose then
              printf " Changed config to %s (committed %s)\n"
                (C.config node.state |> string_of_config)
                (C.committed_config node.state |> string_of_config);

            (* If a Simple_config has been committed, remove nodes no longer
             * active from node set. We also check in the most recent
             * configuration because the committed config could be severely
             * out of date if the node had received a snapshot.
             * *)
            begin match C.config node.state, C.committed_config node.state with
                Joint_config _, _ | _, Joint_config _ -> ()
              | Simple_config (c1, p1), Simple_config (c2, p2) ->
                  (* Stop nodes removed from configuration. Needed when the
                   * node removed was not the leader and thus never gets the
                   * Stop action, since it is no longer in the configuration
                   * by the time the leader sends the Append_entries message
                   * that would let it commit the configuration change. *)
                  let module S = Set.Make(String) in
                  let all = List.concat [c1; p1; c2; p2] |> List.map fst |>
                            List.fold_left (fun s x -> S.add x s) S.empty in

                  let to_be_removed = ref [] in

                  let collect_removee id n =
                    if not (S.mem id all) then begin
                      n.stopped <- true;
                      to_be_removed := id :: !to_be_removed
                    end in

                  let () = NODES.iter collect_removee t.nodes in
                    List.iter (NODES.remove t.nodes) !to_be_removed
            end
        | Exec_readonly n ->
            if verbose then printf " Exec_readonly %Ld\n" n
        | Redirect (Some leader, cmd) ->
            if verbose then printf " Redirect %s\n" leader;
            send_cmd ~dst:leader cmd
        | Redirect (None, cmd) ->
            if verbose then printf " Redirect\n";
            (* Send to a random server. *)
            (* We use a large redirection delay to speed up the simulation:
             * otherwise, most of the time is spent simulating redirections,
             * since there's no time to elect a new leader before the next
             * attempt. This makes sense in practice too, because when you
             * know there's no leader, you don't want to retry until you're
             * confident there's one --- and it's assumed the election period
             * is picked suitably to the network performance. *)
            send_cmd ~dt:CLOCK.(t.election_period / 2L) cmd
        | Reset_election_timeout ->
            if verbose then printf " Reset_election_timeout\n";
            unschedule_election t node;
            schedule_election t node
        | Reset_heartbeat ->
            if verbose then printf " Reset_heartbeat\n";
            unschedule_heartbeat t node;
            schedule_heartbeat t node
        | Send (rep_id, _addr, msg) ->
            let dropped = FAILURE_SIMULATOR.is_msg_lost fail_sim
                            ~src:node.id ~dst:rep_id
            in
              if verbose then
                printf " Send to %S <- %s%s\n" rep_id
                  (string_of_msg string_of_cmd msg)
                  (if dropped then " DROPPED" else "");
              if not dropped then begin
                let dt = Int64.(t.rtt - t.rtt / 4L +
                                of_int (RND.int t.rng (to_int t.rtt lsr 1)))
                in
                  ignore (Event_queue.schedule t.ev_queue dt rep_id
                            (Message (node.id, msg)))
              end
        | Send_snapshot (dst, _addr, idx, config) ->
            if verbose then
              printf " Send_snapshot (%S, %Ld)\n" dst idx;
            let dropped = FAILURE_SIMULATOR.is_msg_lost fail_sim
                            ~src:node.id ~dst
            in
              if not dropped then begin
                let last_index, snapshot, last_term = take_snapshot node in
                let dt = Int64.(t.rtt - t.rtt / 4L +
                           of_int (RND.int t.rng (to_int t.rtt lsr 1)))
                in
                  ignore begin
                    Event_queue.schedule t.ev_queue dt dst
                      (Install_snapshot
                         (node.id, snapshot, last_term, last_index, config))
                  end
              end else begin
                ignore (Event_queue.schedule t.ev_queue
                          CLOCK.(10L * t.rtt) node.id (Snapshot_send_failed dst))
              end
        | Stop ->
            if verbose then printf " Stop\n";
            (* block events on this node *)
            node.stopped <- true;
            ()

      in
        node.state <- s;
        List.iter exec_action actions;
        considered
    in

    let steps = ref 0 in
      (* schedule initial election timeouts *)
      NODES.iter (fun _ n -> schedule_election t n) t.nodes;

      try
        let rec loop () =
          match Event_queue.next t.ev_queue with
              None -> !steps
            | Some (time, rep_id, ev) ->
                match NODES.find t.nodes rep_id, ev with
                    None, Func f ->
                      f time;
                      loop ()
                  | None, _ -> loop ()
                  | Some n, _ ->
                      let blocked = match ev with Func _ -> false | _ -> n.stopped in
                        if not blocked && react_to_event time n ev then incr steps;
                        loop ()
        in loop ()
      with Exit -> !steps
end

module FQueue :
sig
  type 'a t

  val empty : 'a t
  val push : 'a -> 'a t -> 'a t
  val length : 'a t -> int
  val to_list : 'a t -> 'a list
end =
struct
  type 'a t = int * 'a list

  let empty          = (0, [])
  let push x (n, l)  = (n + 1, x :: l)
  let length (n, _)  = n
  let to_list (_, l) = List.rev l
end

let run
      ?(seed = 2)
      ~num_nodes ?(num_cmds = 100_000)
      ~election_period ~heartbeat_period ~rtt ~msg_loss_rate
      ?(verbose=false) () =
  let module S = Set.Make(String) in

  let completed = ref S.empty in
  let init_cmd  = 1 in
  let last_sent = ref init_cmd in
  let ev_queue  = DES.Event_queue.create () in

  let batch_size   = 20 in
  let retry_period = CLOCK.(4L * election_period) in

  let applied = BatBitSet.create (2 * num_cmds) (* work around BatBitSet bug *) in

  let rng     = Random.State.make [| seed |] in

  let des     = DES.make ~ev_queue ~rng ~num_nodes
                  ~election_period ~heartbeat_period ~rtt
                  (fun () -> (0L, FQueue.empty, 0L)) in

  let rec schedule dt node cmd =
    let _    = DES.Event_queue.schedule ev_queue dt node (DES.Command cmd) in
    (* after the retry_period, check if the cmd has been executed
     * and reschedule if needed *)

    let f _  =
      if not (BatBitSet.mem applied cmd) then
        schedule election_period (DES.random_node_id des) cmd in

    let _    = DES.Event_queue.schedule ev_queue
                 CLOCK.(dt + retry_period)
                 (DES.random_node_id des) (DES.Func f)
    in () in

  let check_if_finished node len =
    if len >= num_cmds && List.mem node (DES.live_nodes des) then begin
      completed := S.add (DES.node_id node) !completed;
      if S.cardinal !completed >= num_nodes then
        raise Exit
    end in

  let apply_one ~time node acc (index, cmd, term) =
    if cmd mod (if verbose then 1 else 10_000) = 0 then
      printf "XXXXXXXXXXXXX apply %S  cmd:%d index:%Ld term:%Ld @ %Ld\n%!"
        (DES.node_id node) cmd index term time;
    let q    = match acc with | `Snapshot (_, (_, q, _)) | `State (_, q, _) -> q in
    let id   = DES.node_id node in
    let q    = FQueue.push cmd q in
    let len  = FQueue.length q in
      BatBitSet.set applied cmd;
      if cmd >= !last_sent then begin
      (* We schedule the next few commands being sent to the current leader
       * (simulating the client caching the current leader).  *)
        let dt  = CLOCK.(heartbeat_period - 10L) in
        let dst = Option.default id (DES.leader_id node) in
          for i = 1 to batch_size do
            incr last_sent;
            let cmd = !last_sent in
              schedule CLOCK.(of_int i * dt) dst cmd
          done
      end;
      check_if_finished node len;
      if cmd mod 10 = 0 then begin
        if verbose then
          printf "XXXXX snapshot %S %Ld (last cmds: %s)\n" id index
            (FQueue.to_list q |> List.rev |> List.take 5 |> List.rev |>
             List.map string_of_int |> String.concat ", ");
        `Snapshot (index, (index, q, term))
      end else
        `State (index, q, term)
  in

  let on_apply ~time node cmds =
    List.fold_left (apply_one ~time node) (`State (DES.app_state node)) cmds in

  let take_snapshot node =
    if verbose then printf "TAKE SNAPSHOT at %S\n" (DES.node_id node);
    let (index, _, term) as snapshot = DES.app_state node in
      (index, snapshot, term) in

  let install_snapshot node ((last_index, q, last_term) as snapshot) =
    if verbose then
      printf "INSTALL SNAPSHOT at %S last_index:%Ld last_term:%Ld\n"
        (DES.node_id node) last_index last_term;
    DES.set_app_state node snapshot;
    check_if_finished node (FQueue.length q)
  in

  (* schedule init cmd delivery *)
  let ()    = schedule 1000L (DES.random_node_id des) init_cmd in
  let t0    = Unix.gettimeofday () in
  let steps = DES.simulate
                ~verbose ~string_of_cmd:string_of_int
                ~msg_loss_rate
                ~on_apply ~take_snapshot ~install_snapshot
                des in
  let dt    = Unix.gettimeofday () -. t0 in
  (* We filter out nodes with 0 entries, i.e. those that were being introduced
   * into the cluster at the end of the simulation. *)
  let nodes = DES.live_nodes des |>
              List.filter
                (fun node ->
                   let _, q, _ = DES.app_state node in
                     FQueue.length q <> 0) in
  let ncmds = nodes |>
              List.map
                (fun node ->
                   let _, q, _ = DES.app_state node in
                     printf "%S: len %d\n" (DES.node_id node) (FQueue.length q);
                     FQueue.length q) |>
              List.fold_left min max_int in
  let logs  = nodes |>
              List.map
                (fun node ->
                   let _, q, _ = DES.app_state node in
                     FQueue.to_list q |> List.take ncmds) in
  let ok, _ = List.fold_left
                (fun (ok, l) l' -> match l with
                     None -> (ok, Some l')
                   | Some l -> (ok && l = l', Some l'))
                (true, None)
                logs
    in
      printf "%d commands\n" ncmds;
      printf "Simulated %d steps (%4.2f steps/cmd, %.0f steps/s, %.0f cmds/s).\n"
        steps (float steps /. float ncmds)
        (float steps /. dt) (float ncmds /. dt);
      if ok then
        print_endline "OK"
      else begin
        print_endline "FAILURE: replicated logs differ";
        List.iteri
          (fun n l ->
             let all_eq, _ = List.fold_left
                               (fun (b, prev) x -> match prev with
                                    None -> (true, Some x)
                                  | Some x' -> (b && x = x', Some x))
                               (true, None) l in
             let desc      = if all_eq then "" else " DIFF" in
             let s         = List.map string_of_int l |> String.concat "    " in
               printf "%8d %s%s\n" n s desc)
          (List.transpose logs);
        exit 1;
      end

let params =
  [|
    800L,   200L, 50L, 0.01;
    4000L,  200L, 50L, 0.20;
    8000L,  200L, 50L, 0.50;
    40000L, 200L, 50L, 0.60;
    (* 40000L, 200L, 50L, 0.70; *)
  |]

let random rng a = a.(RND.int rng (Array.length a))

let () =
  for i = 1 to 100 do
    let rng       = RND.make [| i |] in
    let num_nodes = 1 + RND.int rng 5 in
    let election_period, heartbeat_period, rtt, msg_loss_rate = random rng params in
      print_endline (String.make 78 '=');
      printf "seed:%d num_nodes:%d election:%Ld heartbeat:%Ld rtt:%Ld loss:%.4f\n%!"
        i num_nodes election_period heartbeat_period rtt msg_loss_rate;
      run ~seed:i ~verbose:false
        ~num_nodes ~election_period ~heartbeat_period ~rtt ~msg_loss_rate
        ();
      print_endline "";
  done
