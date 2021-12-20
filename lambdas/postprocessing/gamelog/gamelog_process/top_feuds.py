from gamelog_process.award_class import AwardClass

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


class TopFeuds(AwardClass):
    """Calculate the biggest kill streak ove y kills  in x seconds."""

    award_name = "TopFeud"

    def __init__(self):
        super().__init__(self.award_name)
        self.a_b = {}
        self.top_number = 12


    def get_custom_results(self):
        processed_keys = []
        merged = {}
        for key, value in self.a_b.items():
            if key in processed_keys:
                # print("seen that one")
                continue
            if "8e6a51baf1c7e338a118d9e32472954e" in key or "58e419de5a8b2655f6d48eab68275db5" in key:
                # print("sharedguid")
                continue
            reverse_key = key.split("#")[1] + "#" + key.split("#")[0]
            if reverse_key not in self.a_b:
                # print("left")
                merged[key] = [value, 0, value]
            else:
                # print("right")
                merged[key] = [value, self.a_b[reverse_key], value + self.a_b[reverse_key]]
            processed_keys.append(reverse_key)
        
        top_x_marker = 0
        if len(merged) > self.top_number:
            sums = []
            for a_b_sum in merged.values():
                sums.append(a_b_sum[2])  
            top_x_marker = sorted(sums, reverse=True)[self.top_number - 1]
        
        result = []
        for key, a_b_sum in merged.items():
            if a_b_sum[2] >= top_x_marker:
                result.append([key.split("#")[0], key.split("#")[1],a_b_sum[0], a_b_sum[1]])
             
        return result


    def process_event(self, rtcw_event):
        """Take incoming kill with SMG or Pistol and see if it's a longest one for the killer."""
        try:            
            if rtcw_event.get("label", None) == "kill" and rtcw_event.get("unixtime", "").isnumeric():
                killer = rtcw_event["agent"]
                victim = rtcw_event["other"]
                self.a_b[killer + "#" + victim] = self.a_b.get(killer + "#" + victim, 0) + 1
                   
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            error_msg = template.format(type(ex).__name__, ex.args)
            if self.debug:
                print("error at " + self.__name__ + " process event\n")
                print(error_msg)
