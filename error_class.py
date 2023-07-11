class Error:
    def __init__(self,sensor_name,description):
        self.sensor_name = sensor_name
        self.description = description

    def __repr__(self):
        return f"{self.sensor_name}: {self.description}"