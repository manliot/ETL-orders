import random
from datetime import datetime, timedelta

def random_date_betw(initial_date, final_date):
    start = datetime.strptime(initial_date, '%Y-%m-%d')
    end = datetime.strptime(final_date, '%Y-%m-%d')

    diff = end - start
    
    diff_days = diff.days
    random_days = random.randint(0, diff_days)
    
    random_date = start + timedelta(days=random_days)
    
    return random_date