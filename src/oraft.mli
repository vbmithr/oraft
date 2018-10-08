(** Implentation of the RAFT consensus algorithm.
  *
  * Refer to
  * "In Search of an Understandable Consensus Algorithm", Diego Ongaro and John
  * Ousterhout, Stanford University. (Draft of October 7, 2013).
  * [https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf]
  * *)

module Types :
sig
  type status    = Leader | Follower | Candidate [@@deriving bin_io]
  type term      = int64 [@@deriving bin_io]
  type index     = int64 [@@deriving sexp,bin_io]
  type rep_id    = string [@@deriving sexp,bin_io]
  type client_id = string [@@deriving sexp,bin_io]
  type req_id    = client_id * int64 [@@deriving sexp,bin_io]
  type address   = string [@@deriving sexp,bin_io]

  type config =
      Simple_config of simple_config * passive_peers
    | Joint_config of simple_config * simple_config * passive_peers
  and simple_config = (rep_id * address) list
  and passive_peers = (rep_id * address) list [@@deriving sexp,bin_io]

  type 'a message =
      Request_vote of request_vote
    | Vote_result of vote_result
    | Append_entries of 'a append_entries
    | Append_result of append_result
    | Ping of ping
    | Pong of ping [@@deriving sexp,bin_io]

  and request_vote = {
    term : term;
    candidate_id : rep_id;
    last_log_index : index;
    last_log_term : term;
  }

  and vote_result = {
    term : term;
    vote_granted : bool;
  }

  and 'a append_entries = {
    term : term;
    leader_id : rep_id;
    prev_log_index : index;
    prev_log_term : term;
    entries : (index * ('a entry * term)) list;
    leader_commit : index;
  }

  and 'a entry = Nop | Op of 'a | Config of config

  and append_result =
    {
      term : term;
      result : actual_append_result;
    }

  and actual_append_result =
      Append_success of index (* last log entry included in msg we respond to *)
    | Append_failure of index (* index of log entry preceding those in
                                 message we respond to *)

  and ping = { term : term; n : Int64.t; }

  type 'a action =
      Apply of (index * 'a * term) list
    | Become_candidate
    | Become_follower of rep_id option
    | Become_leader
    | Changed_config
    | Exec_readonly of Int64.t
    | Redirect of rep_id option * 'a
    | Reset_election_timeout
    | Reset_heartbeat
    | Send of rep_id * address * 'a message
    | Send_snapshot of rep_id * address * index * config
    | Stop
end

module Core :
sig
  open Types

  type 'a state

  val make :
    id:rep_id -> current_term:term -> voted_for:rep_id option ->
    log:(index * 'a entry * term) list ->
    config:config -> unit -> 'a state

  val is_single_node_cluster : 'a state -> bool

  val leader_id : 'a state -> rep_id option
  val id        : 'a state -> rep_id
  val status    : 'a state -> status
  val config    : 'a state -> config
  val committed_config : 'a state -> config

  val last_index : 'a state -> index
  val last_term  : 'a state -> term
  val peers      : 'a state -> (rep_id * address) list

  val receive_msg :
    'a state -> rep_id -> 'a message -> 'a state * 'a action list

  val election_timeout  : 'a state -> 'a state * 'a action list
  val heartbeat_timeout : 'a state -> 'a state * 'a action list
  val client_command    : 'a -> 'a state -> 'a state * 'a action list

  (** @return [(state, None)] if the node is not the leader,
    * [(state, Some (id, actions))] otherwise, where [id] identifies the
    * requested read-only operation, which can be executed once an
    * [Exec_readonly m] action with [m >= id] is returned within the same term
    * (i.e., with no intermediate [Become_candidate], [Become_follower] or
    * [Become_leader]). *)
  val readonly_operation :
    'a state -> 'a state * (Int64.t * 'a action list) option

  val snapshot_sent :
    rep_id -> last_index:index -> 'a state -> ('a state * 'a action list)

  val snapshot_send_failed : rep_id -> 'a state -> ('a state * 'a action list)

  val install_snapshot :
    last_term:term -> last_index:index -> config:config -> 'a state ->
    'a state * bool

  val compact_log : index -> 'a state -> 'a state

  module Config :
  sig
    type 'a result =
      [
      | `Already_changed
      | `Cannot_change
      | `Change_in_process
      | `Redirect of (rep_id * address) option
      | `Start_change of 'a state
      | `Unsafe_change of simple_config * passive_peers
      ]

    val add_failover    : rep_id -> address -> 'a state -> 'a result
    val remove_failover : rep_id -> 'a state -> 'a result
    val decommission    : rep_id -> 'a state -> 'a result
    val demote          : rep_id -> 'a state -> 'a result
    val promote         : rep_id -> 'a state -> 'a result
    val replace : replacee:rep_id -> failover:rep_id -> 'a state -> 'a result
  end
end
