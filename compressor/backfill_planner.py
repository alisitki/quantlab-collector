"""
QuantLab Backfill Planner
Logic for selecting dates to process in catch-up or reverse backfill modes.
"""

import logging
from typing import List, Set, Optional
from datetime import datetime

from state_semantics import entry_counts_as_complete

logger = logging.getLogger(__name__)

class BackfillPlanner:
    def __init__(self, raw_dates: Set[str], state_manager, today: str):
        self.raw_dates = sorted(list(raw_dates))
        self.state_manager = state_manager
        self.state = state_manager._read_state()
        self.today = today

    def get_completed_dates(self) -> Set[str]:
        """Dates considered done by shared completion semantics."""
        # 1. Check day-level status
        completed = set()
        day_states = self.state.get("days", {})
        for date, entry in day_states.items():
            if entry_counts_as_complete(entry):
                completed.add(date)

        # 2. Check partition-level status
        partition_states = self.state.get("partitions", {})
        date_map = {}
        
        # Group partition statuses by date
        for key, entry in partition_states.items():
            date = key.split('/')[-1]
            if date in completed:
                continue
            date_map.setdefault(date, []).append(entry_counts_as_complete(entry))
            
        for date, statuses in date_map.items():
            if statuses and all(statuses):
                completed.add(date)
                
        return completed

    def plan_reverse(self) -> List[str]:
        """Find pending dates in raw (before today) that are not completed, newest first"""
        completed = self.get_completed_dates()
        
        # Sort raw dates descending and filter completed
        sorted_raw = sorted([d for d in self.raw_dates if d < self.today], reverse=True)
        pending = [d for d in sorted_raw if d not in completed]
        
        return pending

    def plan_catch_up(self) -> List[str]:
        """Forward catch-up from last_compacted_date to today-1"""
        last_date = self.state_manager.get_last_compacted_date()
        if not last_date:
            return []
            
        missing = [d for d in self.raw_dates if d < self.today and d > last_date]
        return missing
