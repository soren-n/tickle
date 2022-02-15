# External module dependencies
from typing import Tuple, List, Dict, Set

# Internal module dependencies
from .evaluate import Task, Program, Batch

###############################################################################
# Functions
###############################################################################
def has_cycle(worklist : List[Task]):
    checked : Set[Task] = set()
    for root_node in worklist:
        path : List[Task] = [root_node]
        stack : List[List[Task]] = [list(root_node.get_deps())]
        while len(stack) != 0:
            node_deps = stack[-1]
            if len(node_deps) == 0:
                checked.add(path.pop(-1))
                stack.pop(-1)
                continue
            node_dep = node_deps.pop(0)
            if node_dep in checked: continue
            if node_dep in path: return True
            path.append(node_dep)
            stack.append(list(node_dep.get_deps()))
    return False

Graph = List[Task]

def get_leafs(graph : Graph):
    return list(filter(lambda node: len(node.get_deps()) == 0, graph))

def get_roots(graph : Graph):
    return list(filter(lambda node: len(node.get_refs()) == 0, graph))

def propagate(graph : Graph):
    def __topological_order(worklist : List[Task]):
        result : List[Task] = list()
        while len(worklist) != 0:
            node = worklist.pop(0)
            result_set = set(result)
            if node in result_set: continue
            if len(node.get_deps().difference(result_set)) != 0: continue
            worklist += node.get_refs().difference(result_set)
            result.append(node)
        return result

    # Defensively check for cycles
    worklist = get_leafs(graph)
    if has_cycle(worklist[:]):
        raise RuntimeError('Cycle detected in agenda!')

    # Propagate invalidity
    for dst in __topological_order(worklist):
        for src in dst.get_deps():
            if not src.get_valid(): dst.set_valid(False)
            if not src.get_active(): dst.set_active(False)

def compile(graph : Graph) -> Program:

    # Find tasks that needs to be performed
    def _reachable_alive(worklist : List[Task]) -> Set[Task]:
        result : Set[Task] = set()
        while len(worklist) != 0:
            task = worklist.pop(0)
            if task in result: continue
            if task.get_valid(): continue
            if not task.get_active(): continue
            result.add(task)
            worklist += task.get_deps()
        return result

    # Find initial tasks
    def _find_leafs(worklist : List[Task], alive : Set[Task]) -> List[Task]:
        result : List[Task] = list()
        visited : Set[Task] = set()
        while len(worklist) != 0:
            task = worklist.pop(0)
            if task in visited: continue
            if task not in alive: continue
            visited.add(task)
            alive_deps = task.get_deps().intersection(alive)
            if len(alive_deps) == 0: result.append(task)
            else: worklist += alive_deps
        return result

    # Join sequential tasks
    def _join_tasks(
        worklist : List[Task],
        alive : Set[Task]
        ) -> Tuple[List[List[Task]], Dict[Task, int]]:
        def _should_join(task : Task, alive_deps : List[Task]) -> bool:
            if len(alive_deps) != 1: return False
            dep = alive_deps[0]
            if len(dep.get_refs().intersection(alive)) != 1: return False
            dep_flows = dep.get_flows()
            for flow, stage in task.get_flows().items():
                if flow not in dep_flows: continue
                if stage == dep_flows[flow]: continue
                return False
            return True

        result : List[List[Task]] = list()
        seqs : Dict[Task, int] = dict()
        visited : Set[Task] = set()
        while len(worklist) != 0:
            task = worklist.pop(0)
            if task in visited: continue
            visited.add(task)
            worklist += task.get_refs().intersection(alive)
            alive_deps = list(task.get_deps().intersection(alive))
            if _should_join(task, alive_deps):
                index = seqs[alive_deps[0]]
                result[index].append(task)
                seqs[task] = index
            else:
                index = len(result)
                result.append([task])
                seqs[task] = index
        return result, seqs

    # Sort task seqs in topological order
    def _parallel_ordering(
        seqs : List[List[Task]],
        seq_map : Dict[Task, int]
        ) -> Program:
        def _stage_batches(
            leafs : List[Task],
            alive : Set[Task]
            ) -> List[List[Batch]]:
            def _seq_refs(seq : int, alive : Set[Task]) -> Set[int]:
                last = seqs[seq][-1]
                alive_refs = last.get_refs().intersection(alive)
                return set( seq_map[ref] for ref in alive_refs )

            def _seq_deps(seq : int, alive : Set[Task]) -> Set[int]:
                first = seqs[seq][0]
                alive_deps = first.get_deps().intersection(alive)
                return set( seq_map[dep] for dep in alive_deps )

            def _add_work(worklist : List[int], work : Set[int]):
                if len(work) == 0: return
                for item in work:
                    if item in worklist: continue
                    worklist.append(item)

            result : Dict[int, Set[int]] = dict()
            batches : Dict[int, int] = dict()
            visited : Set[int] = set()
            worklist : List[int] = list()
            _add_work(worklist, set( seq_map[leaf] for leaf in leafs ))
            while len(worklist) != 0:
                seq = worklist.pop(0)
                if seq in visited: continue
                seq_deps = _seq_deps(seq, alive)
                if len(seq_deps.difference(visited)) != 0: continue
                visited.add(seq)
                _add_work(worklist, _seq_refs(seq, alive))
                batch = max([-1] + [batches[dep] for dep in seq_deps]) + 1
                if batch not in result: result[batch] = set()
                result[batch].add(seq)
                batches[seq] = batch
            return [
                [ seqs[seq] for seq in result[batch] ]
                for batch in range(len(result))
            ]

        def _uid_deps(
            seq : List[Task],
            uid_by_seq : Dict[int, int],
            alive : Set[Task]
            ) -> Set[int]:
            return {
                uid_by_seq[id(seqs[seq_map[node]])]
                for node in seq[0].get_deps().intersection(alive)
            }

        def _uid_refs(
            seq : List[Task],
            uid_by_seq : Dict[int, int],
            alive : Set[Task]
            ) -> Set[int]:
            return {
                uid_by_seq[id(seqs[seq_map[node]])]
                for node in seq[-1].get_refs().intersection(alive)
            }

        def _combine_batches(
            batches_by_flow : Dict[str, List[List[Batch]]]
            ) -> List[List[List[Task]]]:
            def _leafs(graph : Dict[int, Set[int]]) -> List[int]:
                result : List[int] = list()
                for src, dsts in graph.items():
                    if len(dsts) != 0: continue
                    result.append(src)
                return result

            def _add_work(worklist : List[int], work : Set[int]):
                if len(work) == 0: return
                for item in work:
                    if item in worklist: continue
                    worklist.append(item)

            def _batches(
                worklist : List[int],
                deps : Dict[int, Set[int]],
                refs : Dict[int, Set[int]]
                ) -> List[Set[int]]:
                result : Dict[int, Set[int]] = dict()
                batches : Dict[int, int] = dict()
                visited : Set[int] = set()
                while len(worklist) != 0:
                    node = worklist.pop(0)
                    if node in visited: continue
                    node_deps = deps[node]
                    if len(node_deps.difference(visited)) != 0: continue
                    visited.add(node)
                    _add_work(worklist, refs[node])
                    batch = max([-1] + [batches[dep] for dep in node_deps]) + 1
                    if batch not in result: result[batch] = set()
                    result[batch].add(node)
                    batches[node] = batch
                return [ result[batch] for batch in range(len(result)) ]

            # Assign each seq an id
            uid_by_seq : Dict[int, int] = {
                id(seq) : uid for uid, seq in enumerate(seqs)
            }
            graph_deps : Dict[int, Set[int]] = dict()
            graph_refs : Dict[int, Set[int]] = dict()
            alive = set(seq_map.keys())
            for uid, seq in enumerate(seqs):
                graph_deps[uid] = _uid_deps(seq, uid_by_seq, alive)
                graph_refs[uid] = _uid_refs(seq, uid_by_seq, alive)

            # Build seq graph
            for batches in batches_by_flow.values():
                prev_ids = None
                for batch in batches:
                    curr_ids = { uid_by_seq[id(seq)] for seq in batch }
                    if prev_ids is None: prev_ids = curr_ids; continue
                    for curr_id in curr_ids:
                        curr_deps = graph_deps[curr_id]
                        graph_deps[curr_id] = curr_deps.union(prev_ids)
                    for prev_id in prev_ids:
                        prev_refs = graph_refs[prev_id]
                        graph_refs[prev_id] = prev_refs.union(curr_ids)
                    prev_ids = curr_ids

            # Rebuild combined batches
            leafs = _leafs(graph_deps)
            batches = _batches(leafs[:], graph_deps, graph_refs)
            return [
                [ seqs[uid] for uid in batch ]
                for batch in batches
            ]

        # Sort by flow
        seqs_by_flow : Dict[str, List[List[Task]]] = dict()
        for seq in seqs:
            for flow in seq[0].get_flows().keys():
                if flow not in seqs_by_flow: seqs_by_flow[flow] = list()
                seqs_by_flow[flow].append(seq)

        # Find batches for each flow separately
        batches_by_flow : Dict[str, List[List[Batch]]] = dict()
        for flow in seqs_by_flow.keys():

            # Sort by stage
            alive_by_stage : Dict[int, Set[Task]] = dict()
            seq_by_stage : Dict[int, List[List[Task]]] = dict()
            for seq in seqs_by_flow[flow]:
                stage = seq[0].get_flows()[flow]
                if stage not in alive_by_stage:
                    alive_by_stage[stage] = set()
                    seq_by_stage[stage] = list()
                alive_by_stage[stage] = alive_by_stage[stage].union(set(seq))
                seq_by_stage[stage].append(seq)

            # Roots of each stage
            leafs_by_stage : Dict[int, List[Task]] = dict()
            for stage in alive_by_stage.keys():
                stage_seq = seq_by_stage[stage]
                stage_alive = alive_by_stage[stage]
                worklist = [ seq[0] for seq in stage_seq ]
                leafs_by_stage[stage] = _find_leafs(worklist, stage_alive)

            # Find batches of each stage
            batches : List[List[Batch]] = list()
            for stage in sorted(list(alive_by_stage.keys())):
                stage_leafs = leafs_by_stage[stage]
                stage_alive = alive_by_stage[stage]
                batches += _stage_batches(stage_leafs, stage_alive)

            # Set batches for flow
            batches_by_flow[flow] = batches

        # Combine batches
        return _combine_batches(batches_by_flow)

    targets = get_roots(graph)
    if has_cycle(targets[:]):
        raise RuntimeError('Cycle detected in agenda!')
    alive = _reachable_alive(targets[:])
    roots = _find_leafs(targets[:], alive)
    seqs, seq_map = _join_tasks(roots[:], alive)
    return _parallel_ordering(seqs, seq_map)
