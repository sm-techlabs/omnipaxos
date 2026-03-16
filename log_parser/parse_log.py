import re
from datetime import datetime
# import matplotlib.pyplot as plt
# import networkx as nx
from enum import Enum


class PATH(Enum):
    FAST = 1
    SLOW = 2

def parse_file(filename):
    # Parse the log file and build the timeline map
    log_file = filename 
    timeline_map = {}

    with open(log_file, 'r') as f:
        for line in f:
            line = line.strip()
            # AI is really smart guys...
            if not line or not line.startswith('Mar'):
                continue  # Skip non-log lines
            # Split the line: timestamp level message
            parts = line.split(' ')
            if len(parts) < 3:
                continue
            if parts[4][0] != '[':
                continue
            timestamp = ' '.join(parts[:2])  # Mar 13 15:04:35.158
            level = parts[3]  # INFO
            event = parts[4] 

            # get request id and pairs
            request_id = None
            details = []
            for p in parts[5:]:
                if "request=" in p:
                    request_id = int(p.split("=")[1])
                else:
                    details.append(p)
            if request_id is None:
                continue 
            
            event_data = {
                'timestamp': timestamp,
                'event_type': event,
                'details': details,
                'request_id': request_id,
            }
            if request_id not in timeline_map:
                timeline_map[request_id] = []
            timeline_map[request_id].append(event_data)
        return timeline_map

def draw_digraph_timeline(timeline_map):
    # Draw the timeline as a graph for each request_id
    for request_id, events in timeline_map.items():
        G = nx.DiGraph()
        labels = {}
        prev_node = None
        for idx, event in enumerate(events):
            node = f"{request_id}_{idx}"
            details_str = ', '.join(f"{k}={v}" for k, v in event['details'].items())
            label = f"{event['timestamp']}\n{event['event_type']}\n{details_str}"
            G.add_node(node)
            labels[node] = label
            if prev_node:
                G.add_edge(prev_node, node)
            prev_node = node
        plt.figure(figsize=(10, 2))
        pos = nx.spring_layout(G, seed=42, k=2)
        nx.draw(G, pos, with_labels=False, node_color='lightblue', node_size=2000, arrows=True)
        nx.draw_networkx_labels(G, pos, labels, font_size=8)
        plt.title(f"Timeline for Request ID: {request_id}")
        plt.axis('off')
        plt.show()

def remove_esc(line: str) -> str:
    ansi_escape = re.compile(r'\x1b\[[0-9;]*m')
    return ansi_escape.sub(' ', line)

#s1  | ESC[31m[P1 Fol/Acc]ESC[0m Mar 15 12:12:38.653 INFO  [INFO][FAST_PATH][BUFFER] releasing request=12702945265148441474 coordinator=2 deadline=1773576757663324
def parse_line(line: str):
    line = remove_esc(line.strip())
    parts = line.split(" ")
    event = {}
    event["pidE"] = ""
    kv = {}
    single = []
    for i, part in enumerate(parts):
        if len(part) < 3:
            continue
        if part[:2] == "[P":
            event["pidE"] = (" ").join([part, parts[i+1]])
        elif "request=" in part:
            rid = part.split("=")[1]
            event["rid"] = rid
        elif "=" in part:
            kvs = part.split("=")
            kv[kvs[0]] = kvs[1]
        elif "[" in part and "]" in part:
            event["info"] = part
        elif ":" in part: 
            try:
                ts = (" ").join([parts[i-2], parts[i-1], part])
                dt = datetime.strptime(ts, "%b %d %H:%M:%S.%f")
                current_year = datetime.now().year
                dt = dt.replace(year=current_year)
                event["timestamp"] = dt
            except:
                continue
        else:
            single.append(part)
    event["kv_pairs"] = kv
    event["single"] = single
    return event

def get_all_events(filename):
    with open(filename, "r") as f:
        events = []
        for line in f.readlines():
            events.append(parse_line(line))
    return events

def sort_events(events: list[dict]):
    rid_to_idx = {}
    events_at_id = {}
    unknown_events = []
    # initial pass using request id
    while events:
        ev = events.pop()
        if "rid" in ev.keys() and "accepted_idx" in ev['kv_pairs'].keys() and "Ldr" in ev["pidE"] and "[SEND]" in ev["info"]:
            rid = ev["rid"]
            idx = ev['kv_pairs']['accepted_idx']
            idx = int(idx.replace("Some(", "").replace(")", ""))
            if rid not in rid_to_idx:
                rid_to_idx[rid] = idx
            else:
                if idx != rid_to_idx[rid]:
                    ori = events_at_id[rid][0]
                    raise Exception(f"RID: {rid} does not match accepted idx {rid_to_idx[rid]}\n{ev}\n{ori}") 
            events_at_id.setdefault(rid, []).append(ev)
        elif "rid" in ev.keys():
            rid = ev["rid"]
            events_at_id.setdefault(rid, []).append(ev)
        else:
            unknown_events.append(ev)
    # second pass using accepted idx or decided idx
    for ev in unknown_events:
        idx = None
        if "accepted_idx" in ev["kv_pairs"].keys():
            idx = int(ev["kv_pairs"]["accepted_idx"].replace("Some(", "").replace(")", ""))
        elif "decided_idx" in ev["kv_pairs"].keys():
            idx = int(ev["kv_pairs"]["decided_idx"].replace("Some(", "").replace(")", "").replace(":", "").replace(",", ""))
        # elif "pidE" in ev.keys():
        #     print(f"Could not link event to request id: {ev}")
        if idx != None:
            # not efficient
            for k,v in rid_to_idx.items():
                if v == idx:
                    rid = k
                    events_at_id[rid].append(ev)
    return events_at_id


def find_match_event(events: list[dict]):
    for event in events:
        if "rid" in event.keys() and "accepted_idx" in event['kv_pairs'].keys():
            print(event)
            break

def calc_latency(revents: list[dict]) -> tuple[float, Enum, float, float]:
    start = None
    end = None
    type = None
    for ev in revents:
        if "info" in ev.keys():
            if "FAST_PROPOSE" in ev["info"] and start is None:
                start = ev
            elif "[INFO][FAST_PATH][DECIDE]" in ev["info"]:
                end = ev
                type = PATH.FAST
            elif "[LEADER][SLOW_PATH]" in ev ["info"] and start is None:
                end = ev
                type = PATH.SLOW
            elif "[SEND][DECIDE]" in ev["info"] and end is None:
                end = ev
                type = PATH.SLOW
    if start is None or end is None:
        return None, None, None, None
    latency = (end["timestamp"].timestamp() - start["timestamp"].timestamp())*1000
    return latency, type, start["timestamp"].timestamp(), end["timestamp"].timestamp()

def has_proposal(revents: list[dict]):
    for ev in revents:
        if "info" in ev.keys():
            if "FAST_PROPOSE" in ev["info"]:
                return True
    return False

def count_hash_errors(revents: list[dict]) -> bool:
    for ev in revents:
        if "info" in ev.keys() and "[HASH_MISMATCH]" in ev["info"]:
            return True
    return False


def get_proposals_and_mean_latency(revents: list[list[dict]]) -> tuple[int, int]:
    prop_count = 0
    latencies = []
    paths = []
    first = 100000000000000000000
    last = 0
    hash_errors = 0
    for prop in revents:
        if has_proposal(prop):
            prop_count += 1
            latency, type, start, end = calc_latency(prop)
            if count_hash_errors(prop):
                hash_errors += 1
            if latency is not None:
                latencies.append(latency)
                paths.append(type)
                if start < first:
                    first = start
                if end > last:
                    last = end
    mean_latency = sum(latencies)/len(latencies)
    fast_comps = sum([1 for x in paths if x == PATH.FAST])
    slow_comps = sum([1 for x in paths if x == PATH.SLOW])
    print(f"{prop_count} Proposals with mean latency of {mean_latency:.3f} over {len(latencies)} completed events")
    print(f"{fast_comps} Fast Path Completions :: {slow_comps} Slow Path Completions")
    end_to_end = last - first
    print(f"Mean Throughput: {len(latencies)/end_to_end:.3f} Requests/s :: Runtime: {end_to_end:.3f} seconds")
    print(f"Hash Errors: {hash_errors}")
    return prop_count, mean_latency

if __name__ == "__main__":
    import sys
    filename = sys.argv[1] if len(sys.argv) > 1 else "log_parser/logs_131.log"
    events = get_all_events(filename)
    event_data = sort_events(events)
    print(f"Number matched events: {len(event_data.keys())}")
    get_proposals_and_mean_latency(event_data.values())
    
