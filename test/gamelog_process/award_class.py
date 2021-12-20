class AwardClass:
    """Calculate the longest kill in the game."""

    def __init__(self, award_name):
        self.award_name = award_name
        self.players_values = {}
        self.debug = False

    def get_full_results(self):
        """Return result as {"Award name": {"guid":"value"}}."""
        self.clean_dups()
        return {self.award_name: self.players_values}
    
    def get_custom_results(self):
        raise NotImplementedError("Method was not implemented.")
        return

    # def get_one_top_result(self):
    #     """Return result as {"Award name": {"guid":value}}."""
    #     top_key = max(self.players_values, key=self.players_values.get)
    #     return {self.award_name: {top_key : self.players_values[top_key]}}

    def get_all_top_results(self):
        """Return result as {"Award name": {"guid":value}}."""
        
        self.clean_dups()
        if len(self.players_values) > 0:
            highest = max(self.players_values.values())
        else:
            return {}
        
        result = {}
        for k, v in self.players_values.items() :
            if v == highest:
                result[k] = v
                
        if len(result) > 0:
            return {self.award_name: result}
        else:
            return {}

    def process_event(self, rtcw_event):
        """User implemented event processing."""
        raise ValueError("Not implemented!")
    
    
    def clean_dups(self):
        dups = ['8e6a51baf1c7e338a118d9e32472954e','58e419de5a8b2655f6d48eab68275db5']
        for d in dups:
            if d in self.players_values:
                del self.players_values[d]
