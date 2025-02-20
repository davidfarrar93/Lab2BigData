#https://github.com/astan54321/PA3/blob/44628868dcc7f00feec9e4c4bdb9391558391ac7/problem2_3.py

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

# Regular expression to match word-like sequences (e.g., species names)
DATA_RE = re.compile(r"[\w.-]+")

class MRHeartDiseaseAnalysis(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_cholesterol,
                   reducer=self.reducer_get_avg_cholesterol)
        ]

    def mapper_get_cholesterol(self, _, line):
        # Split the line by commas (as the dataset is comma-separated)
        data = line.strip().split(',')
        
        # Skip header (if present) and ensure proper data format
        if data[0] == "age":
            return  # Skip header line
        
        try:
            # Extract the age (integer) and cholesterol level (integer)
            age = int(data[0])
            cholesterol = int(data[4])  # Cholesterol is in column 5 (index 4)
            
            # Group by age (for example, 10-year age groups)
            age_group = (age // 10) * 10  # Age group e.g., 30-39, 40-49, etc.
            
            # Yield key-value pairs where the key is the age group and value is the cholesterol level
            yield (age_group, cholesterol)
        except ValueError:
            pass  # Skip invalid data (non-integer values)

    def reducer_get_avg_cholesterol(self, age_group, cholesterol_levels):
        size, total = 0, 0
        
        # Sum up the cholesterol levels and count how many entries there are in the group
        for cholesterol in cholesterol_levels:
            size += 1
            total += cholesterol
        
        # Calculate the average cholesterol level for each age group
        if size > 0:
            avg_cholesterol = round(total / size, 1)
            yield (f"Age group {age_group}-{age_group+9} avg cholesterol", avg_cholesterol)

if __name__ == '__main__':
    MRHeartDiseaseAnalysis.run()
