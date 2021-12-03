import math
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


class LongestKill(AwardClass):
    """Calculate the longest kill in the game."""

    award_name = "Longest Kill"

    def __init__(self):
        super().__init__(self.award_name)

    def process_event(self, rtcw_event):
        """Take incoming kill with SMG or Pistol and see if it's a longest one for the killer."""
        try:
            if rtcw_event.get("label", None) == "kill" and rtcw_event.get("weapon", None) in ["MP-40", "Thompson", "Sten", "Colt", "Luger"]:
                killer = rtcw_event.get("agent", "no guid")
                x1, x2, y1, y2, z1, z2 = self.get_coordinates(rtcw_event)
                dist = int(math.sqrt((x2 - x1)**2 + (y2 - y1)**2 + (z2 - z1)**2))
                if self.players_values.get(killer, 0) < dist:
                    self.players_values[killer] = dist
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            error_msg = template.format(type(ex).__name__, ex.args)
            if self.debug:
                print("error at " + self.__name__ + " process event\n")
                print(error_msg)

    def get_coordinates(self, rtcw_event):
        """Extract coorinates from the position string."""
        x1 = x2 = y1 = y2 = z1 = z2 = 0
        try:
            agent_coord = rtcw_event["agent_pos"].split(",")
            other_coord = rtcw_event["other_pos"].split(",")
            x1 = float(agent_coord[0])
            y1 = float(agent_coord[1])
            z1 = float(agent_coord[2])
            x2 = float(other_coord[0])
            y2 = float(other_coord[1])
            z2 = float(other_coord[2])
        except Exception as ex:
            if self.debug:
                print("error at " + rtcw_event.get("unixtime", "no_unixtime") + ": could not split coordinates." + ex.args)
            # exception handling and reporting is not desired here because of data volumes and who receives them

        return x1, x2, y1, y2, z1, z2
