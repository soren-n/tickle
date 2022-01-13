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
    def __reachable_alive(worklist):
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
    def __find_leafs(worklist, alive):
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

    # Join sequential tasks into groups
    def __join_tasks(worklist, alive):
        def _should_join(task, alive_deps):
            if len(alive_deps) != 1: return False
            return alive_deps[0].get_stage() == task.get_stage()

        result = list()
        groups = dict()
        visited = set()
        while len(worklist) != 0:
            task = worklist.pop(0)
            if task in visited: continue
            visited.add(task)
            worklist += refs(task).intersection(alive)
            alive_deps = list(deps(task).intersection(alive))
            if _should_join(task, alive_deps):
                index = groups[alive_deps[0]]
                result[index].append(task)
                groups[task] = index
            else:
                index = len(result)
                result.append([task])
                groups[task] = index
        return result, groups

    # Sort task groups in topological order
    def __parallel_ordering(groups, group_map):
        def _add_work(worklist, work):
            if len(groups) == 0: return
            for item in work:
                if item in worklist: continue
                worklist.append(item)
            worklist.sort(key = lambda group: groups[group][0].get_stage())

        def __group_refs(group, alive):
            last = groups[group][-1]
            alive_refs = refs(last).intersection(alive)
            return set( group_map[ref] for ref in alive_refs )

        def __group_deps(group, alive):
            first = groups[group][0]
            alive_deps = deps(first).intersection(alive)
            return set( group_map[dep] for dep in alive_deps )

        def __parallel_batches(roots, alive):
            result = dict()
            batches = dict()
            visited = set()
            worklist = list()
            _add_work(worklist, set( group_map[root] for root in roots ))
            while len(worklist) != 0:
                group = worklist.pop(0)
                if group in visited: continue
                group_deps = __group_deps(group, alive)
                if len(group_deps.difference(visited)) != 0: continue
                visited.add(group)
                _add_work(worklist, __group_refs(group, alive))
                batch = max([-1] + [ batches[dep] for dep in group_deps ]) + 1
                if batch not in result: result[batch] = set()
                result[batch].add(group)
                batches[group] = batch
            return [
                [ groups[group] for group in result[batch] ]
                for batch in range(len(result))
            ]

        # Sort by stage
        alive_by_stage = dict()
        groups_by_stage = dict()
        for group in groups:
            stage = group[0].get_stage()
            if stage not in alive_by_stage:
                alive_by_stage[stage] = set()
                groups_by_stage[stage] = list()
            alive_by_stage[stage] = alive_by_stage[stage].union(set(group))
            groups_by_stage[stage].append(group)

        # Roots of each stage
        roots_by_stage = dict()
        for stage in alive_by_stage.keys():
            stage_groups = groups_by_stage[stage]
            stage_alive = alive_by_stage[stage]
            worklist = [group[0] for group in stage_groups]
            roots_by_stage[stage] = __find_leafs(worklist, stage_alive)

        # Find batches of each stage
        result = list()
        for stage in sorted(list(alive_by_stage.keys())):
            stage_roots = roots_by_stage[stage]
            stage_alive = alive_by_stage[stage]
            result += __parallel_batches(stage_roots, stage_alive)

        # Done
        return result

    targets = get_roots(graph)
    if has_cycle(targets[:]):
        raise RuntimeError('Cycle detected in agenda!')
    alive = __reachable_alive(targets[:])
    roots = __find_leafs(targets[:], alive)
    groups, group_map = __join_tasks(roots[:], alive)
    return __parallel_ordering(groups, group_map)
