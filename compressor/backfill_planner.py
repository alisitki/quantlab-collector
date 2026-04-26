"""
QuantLab Backfill Planner
Logic for selecting dates to process in catch-up or reverse backfill modes.
"""

import logging
from typing import List, Set, Optional
from datetime import datetime

from manifest_state import day_fetch_ready, is_v2_state

logger = logging.getLogger(__name__)

class BackfillPlanner:
    def __init__(self, raw_dates: Set[str], state_manager, today: str):
        self.raw_dates = sorted(list(raw_dates))
        self.state_manager = state_manager
        self.state = state_manager._read_state()
        self.today = today

    def get_completed_dates(self) -> Set[str]:
        """Dates considered done by fetch-ready manifest semantics."""
        if is_v2_state(self.state):
            return {date for date in self.state.get("dates", {}) if day_fetch_ready(self.state, date)}

        completed = set()
        day_states = self.state.get("days", {})
        for date, entry in day_states.items():
            if entry.get("counts_as_complete"):
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

        missing = [
            d for d in self.raw_dates
            if d < self.today and d > last_date and not day_fetch_ready(self.state, d)
        ] if is_v2_state(self.state) else [d for d in self.raw_dates if d < self.today and d > last_date]
        return missing
