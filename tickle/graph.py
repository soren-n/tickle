###############################################################################
# Functions
###############################################################################
def deps(task): return task._deps
def refs(task): return task._refs

def has_cycle(worklist):
    def _visit(task, path, visited):
        if task in visited: return False
        if task in path: return True
        _path = path.copy()
        _path.append(task)
        _visited = visited.copy()
        _visited.add(task)
        for dep in deps(task):
            if _visit(dep, _path, _visited): return True
        return False

    visited = set()
    for task in worklist:
        if _visit(task, list(), visited): return True
        visited.add(task)
    return False

def get_leafs(graph):
    return list(filter(lambda node: len(deps(node)) == 0, graph))

def get_roots(graph):
    return list(filter(lambda node: len(refs(node)) == 0, graph))

def propagate(graph):
    def __topological_order(worklist):
        result = list()
        while len(worklist) != 0:
            node = worklist.pop(0)
            result_set = set(result)
            if node in result_set: continue
            if len(deps(node).difference(result_set)) != 0: continue
            worklist += refs(node).difference(result_set)
            result.append(node)
        return result

    # Defensively check for cycles
    worklist = get_leafs(graph)
    if has_cycle(worklist[:]):
        raise RuntimeError('Cycle detected in agenda!')

    # Propagate invalidity
    for dst in __topological_order(worklist):
        for src in deps(dst):
            if not src.get_valid(): dst.set_valid(False)
            if not src.get_active(): dst.set_active(False)

def compile(graph):

    # Find tasks that needs to be performed
    def _reachable_alive(worklist):
        result = list()
        while len(worklist) != 0:
            task = worklist.pop(0)
            if task in result: continue
            if task.get_valid(): continue
            if not task.get_active(): continue
            result.append(task)
            worklist += deps(task)
        return list(reversed(result))

    # Find initial tasks
    def _find_leafs(worklist, alive):
        result = list()
        visited = set()
        while len(worklist) != 0:
            task = worklist.pop(0)
            if task in visited: continue
            if task not in alive: continue
            visited.add(task)
            alive_deps = deps(task).intersection(alive)
            if len(alive_deps) == 0: result.append(task)
            else: worklist += alive_deps
        return result

    # Join sequential tasks
    def _join_tasks(worklist, alive):
        def _should_join(task, alive_deps):
            if len(alive_deps) != 1: return False
            dep = alive_deps[0]
            if len(refs(dep).intersection(alive)) != 1: return False
            dep_flows = dep.get_flows()
            for flow, stage in task.get_flows().items():
                if flow not in dep_flows: continue
                if stage == dep_flows[flow]: continue
                return False
            return True

        result = list()
        seqs = dict()
        visited = set()
        while len(worklist) != 0:
            task = worklist.pop(0)
            if task in visited: continue
            visited.add(task)
            worklist += refs(task).intersection(alive)
            alive_deps = list(deps(task).intersection(alive))
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
    def _parallel_ordering(seqs, seq_map):
        def _stage_batches(leafs, alive):
            def _seq_refs(seq, alive):
                last = seqs[seq][-1]
                alive_refs = refs(last).intersection(alive)
                return set( seq_map[ref] for ref in alive_refs )

            def _seq_deps(seq, alive):
                first = seqs[seq][0]
                alive_deps = deps(first).intersection(alive)
                return set( seq_map[dep] for dep in alive_deps )

            def _add_work(worklist, work):
                if len(work) == 0: return
                for item in work:
                    if item in worklist: continue
                    worklist.append(item)

            result = dict()
            batches = dict()
            visited = set()
            worklist = list()
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

        def _uid_deps(seq, uid_by_seq, alive):
            return {
                uid_by_seq[id(seqs[seq_map[node]])]
                for node in deps(seq[0]).intersection(alive)
            }

        def _uid_refs(seq, uid_by_seq, alive):
            return {
                uid_by_seq[id(seqs[seq_map[node]])]
                for node in refs(seq[-1]).intersection(alive)
            }

        def _combine_batches(batches_by_flow):
            def _leafs(graph):
                result = list()
                for src, dsts in graph.items():
                    if len(dsts) != 0: continue
                    result.append(src)
                return result

            def _add_work(worklist, work):
                if len(work) == 0: return
                for item in work:
                    if item in worklist: continue
                    worklist.append(item)

            def _batches(worklist, deps, refs):
                result = dict()
                batches = dict()
                visited = set()
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
            uid_by_seq = { id(seq) : uid for uid, seq in enumerate(seqs) }
            graph_deps = dict()
            graph_refs = dict()
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
        seqs_by_flow = dict()
        for seq in seqs:
            for flow in seq[0].get_flows().keys():
                if flow not in seqs_by_flow: seqs_by_flow[flow] = list()
                seqs_by_flow[flow].append(seq)

        # Find batches for each flow separately
        batches_by_flow = dict()
        for flow in seqs_by_flow.keys():

            # Sort by stage
            alive_by_stage = dict()
            seq_by_stage = dict()
            for seq in seqs_by_flow[flow]:
                stage = seq[0].get_flows()[flow]
                if stage not in alive_by_stage:
                    alive_by_stage[stage] = set()
                    seq_by_stage[stage] = list()
                alive_by_stage[stage] = alive_by_stage[stage].union(set(seq))
                seq_by_stage[stage].append(seq)

            # Roots of each stage
            leafs_by_stage = dict()
            for stage in alive_by_stage.keys():
                stage_seq = seq_by_stage[stage]
                stage_alive = alive_by_stage[stage]
                worklist = [ seq[0] for seq in stage_seq ]
                leafs_by_stage[stage] = _find_leafs(worklist, stage_alive)

            # Find batches of each stage
            batches = list()
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
