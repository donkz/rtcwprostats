class AwardClass:
    """Calculate the longest kill in the game."""

    def __init__(self, award_name):
        self.award_name = award_name
        self.players_values = {}
        self.debug = False

    def get_full_results(self):
        """Return result as {"Award name": {"guid":"value"}}."""
        return {self.award_name: self.players_values}

    def get_one_top_result(self):
        """Return result as {"Award name": {"guid":value}}."""
        top_key = max(self.players_values, key=self.players_values.get)
        return {self.award_name: {top_key : self.players_values[top_key]}}

    def get_all_top_results(self):
        """Return result as {"Award name": {"guid":value}}."""
        highest = max(self.players_values.values())
        result = {}
        for k, v in self.players_values.items() :
            if v == highest:
                result[k] = v
        return {self.award_name: result}

    def process_event(self, rtcw_event):
        """User implemented event processing."""
        raise ValueError("Not implemented!")
