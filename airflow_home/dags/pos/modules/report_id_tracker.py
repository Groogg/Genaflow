from pos.modules.common import read_json
from datetime import datetime, timedelta
import json

def update_report_tracker(file_path, banner, new_id):
    """
    Opens report_tracker.json and change report_last_id with the new_id and update
    the new report_next_week_starting_date by 7 days (update the modified date for both).

    Args:
        file_path (str): Path the the report_id_tacker.json
        banner (str): Banner acronym (ex: jpc).
        new_id (json compatible type): New report id (see generate_report_id in modules/extract.py).

    Returns: Update the report_last_id and report_last_id_modified_date in report_tracker.json.
    """
    data = read_json(file_path=file_path)

    data[banner]['report_last_id'] = new_id

    current_date = datetime.strptime(data[banner]['report_next_week_starting_date'], '%Y-%m-%d')
    adjusted_date = current_date + timedelta(days=7)
    data[banner]['report_next_week_starting_date'] = str(adjusted_date.strftime("%Y-%m-%d"))

    data[banner]['last_modified_date'] = str(datetime.today().strftime("%Y-%m-%d %H:%M"))
    
    with open(file_path, 'w') as outfile:  
        json.dump(data, outfile)



