import math

"""
relevant json element
{
    match_id: "1630953060",
    round_id: "1",
    unixtime: "1630953652",
    group: "player",
    label: "kill",
    agent: "68deaefc0a07be79fcb2cc5104a71b1e",
    other: "5379320f3c64f43cdaf3350fc13011ce",
    weapon: "MP-40",
    other_health: 125,
    agent_pos: "589.451965,-70.798233,69.000000",
    agent_angle: "79.161987",
    other_pos: "2791.199219,4.931318,188.575974",
    other_angle: "-88.071899",
    allies_alive: "5",
    axis_alive: "1"
}
"""

def my_function(gamelog):
    longest_range = {}
    for rtcw_event in gamelog:
        if rtcw_event.get("label", None) == "kill" and rtcw_event.get("weapon", None) in ["MP-40", "Thompson", "Sten", "Colt", "Luger"]:
            killer = rtcw_event.get("agent","no guid")
            x1, x2, y1, y2, z1, z2 = get_coordinates(rtcw_event) 
            dist = int(math.sqrt((x2 - x1)**2 + (y2 - y1)**2 + (z2 - z1)**2))
            if longest_range.get(killer, 0) < dist:
                longest_range[killer] = dist
    return {"Longest_Range": longest_range}

def get_coordinates(rtcw_event):
    x1 = x2 = y1 = y2 = z1 = z2 = 0
    debug = False
    try:
        agent_coord = rtcw_event["agent_pos"].split(",")
        other_coord = rtcw_event["other_pos"].split(",")
        x1 = float(agent_coord[0])
        y1 = float(agent_coord[1])
        z1 = float(agent_coord[2])
        x2 = float(other_coord[0])
        y2 = float(other_coord[1])
        z2 = float(other_coord[2])
    except:
        if debug: 
            print("error at " + rtcw_event.get("unixtime", "no_unixtime") + ": could not split coordinates.")
        # exception handling and reporting is not desired here because of data volumes and who receives them
    
    return x1, x2, y1, y2, z1, z2
        
    