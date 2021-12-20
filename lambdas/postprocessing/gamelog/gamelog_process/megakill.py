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


class MegaKill(AwardClass):
    """Calculate the biggest kill streak ove y kills  in x seconds."""

    award_name = "MegaKill"

    def __init__(self):
        super().__init__(self.award_name)
        self.current_kills = {}
        self.minimum_seconds = 6
        self.minimum_kills = 3
        self.local_debug = False
        self.debug_killer = "b3465bff43fe40ea76f9e522d3314809"
        self.current_time = 0
        
    def process_event(self, rtcw_event):
        """Take incoming kill with SMG or Pistol and see if it's a longest one for the killer."""
        try:
            if rtcw_event.get("label", None) == "round_start":
                self.current_kills = {}
                if self.local_debug:
                    print("reset all")
            
            if rtcw_event.get("label", None) == "kill" and rtcw_event.get("unixtime", "").isnumeric():
                killer = rtcw_event["agent"]
                self.current_time = int(rtcw_event.get("unixtime", "0"))
                                
                # append current kill time stamp to kill time stamp array
                if self.local_debug and killer == self.debug_killer:
                    print(str(self.current_kills.get(killer,[])) + " + " + str(self.current_time))
                if killer in self.current_kills:
                    self.current_kills[killer].append(self.current_time)
                else:
                    self.current_kills[killer] = [self.current_time]
         
                # remove time stamps older than x seconds or once from other matches
                removals = []  # because removing during iterations is a mess
                for past_time_stamp in self.current_kills[killer]:
                    time_since = self.current_time - past_time_stamp
                    if time_since > self.minimum_seconds or time_since < 0:
                        removals.append(past_time_stamp)
                        if self.local_debug and killer == self.debug_killer:
                            print("will remove " + str(past_time_stamp) + "( " + str(time_since) + " )")
                    else:
                        if self.local_debug and killer == self.debug_killer:
                            print("will keep " + str(past_time_stamp) + "( " + str(time_since) + " )")
                
                for r in removals:
                    self.current_kills[killer].remove(r)
                    if self.local_debug and killer == self.debug_killer:
                            print("removing", str(r))
                
                # see if length of recent kills qualifies for a record
                current_streak = len(self.current_kills.get(killer, []))
                if current_streak >= self.minimum_kills and current_streak > self.players_values.get(killer, 0):
                    self.players_values[killer] = current_streak
                    if self.local_debug and killer == self.debug_killer:
                            print("Record!", str(current_streak))
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            error_msg = template.format(type(ex).__name__, ex.args)
            if self.debug:
                print("error at " + self.__name__ + " process event\n")
                print(error_msg)