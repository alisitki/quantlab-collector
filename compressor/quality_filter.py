"""
QuantLab Quality Post-Filter
Implements window-level post-filtering and day-level quality aggregation.
"""

from typing import List, Dict, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

# Constants
EXPECTED_WINDOWS_PER_DAY = 96
POST_FILTER_VERSION = "1.0.0"

class QualityFilter:
    """
    Handles quality assessment for windows and days.
    """
    
    @staticmethod
    def assess_window(window_json: Dict) -> Dict:
        """
        Assess a single window based on post-filter rules.
        Returns a dict with post_quality and reasons.
        """
        signals = window_json.get("signals", {})
        original_quality = window_json.get("quality", "UNKNOWN")
        is_partial = window_json.get("is_partial", False)
        
        reasons = []
        
        # 1. Extract signals
        dropped_events = signals.get("dropped_events", 0)
        queue_pct_peak = signals.get("queue_pct_peak", 0)
        reconnects = signals.get("reconnects", 0)
        drain_mode_acc = signals.get("drain_mode_accelerated_seconds", 0)
        
        offline_by_ex = signals.get("offline_seconds_by_exchange", {})
        binance_offline = offline_by_ex.get("binance", 0)
        max_offline = max(offline_by_ex.values()) if offline_by_ex else 0
        
        eps_by_ex = signals.get("eps_by_exchange", {})
        binance_eps_min = eps_by_ex.get("binance", {}).get("min")
        
        # zero_eps_seconds is omitted as per instruction
        
        # 2. Assessment logic
        post_quality = "GOOD"
        
        # Hard BAD rules
        is_hard_bad = False
        if dropped_events > 0:
            is_hard_bad = True
            reasons.append(f"dropped_events={dropped_events}")
        if queue_pct_peak >= 90:
            is_hard_bad = True
            reasons.append(f"queue_pct_peak={queue_pct_peak}")
        if binance_offline > 600:
            is_hard_bad = True
            reasons.append(f"binance_offline={binance_offline}")
            
        if is_hard_bad:
            post_quality = "BAD"
        else:
            # DEGRADED rules
            is_degraded = False
            if max_offline > 180:
                is_degraded = True
                reasons.append(f"max_offline={max_offline}")
            if drain_mode_acc > 180:
                is_degraded = True
                reasons.append(f"drain_mode_acc={drain_mode_acc}")
            if reconnects >= 5:
                is_degraded = True
                reasons.append(f"reconnects={reconnects}")
                
            if is_degraded:
                post_quality = "DEGRADED"
                
        # 3. BAD -> DEGRADED Downgrade logic (Wait, user said BAD->DEGRADED downgrade)
        # "dropped_events==0 AND max_offline<300 AND queue_pct_peak<90"
        if post_quality == "BAD":
            if dropped_events == 0 and max_offline < 300 and queue_pct_peak < 90:
                # If it was BAD because of binance_offline > 600, but max_offline < 300? 
                # This seems contradictory if binance is one of the exchanges.
                # However, following the rule literally:
                post_quality = "DEGRADED"
                reasons.append("Downgraded from BAD to DEGRADED (Safe checks)")

        # 4. Overrides (DEGRADED -> GOOD)
        if post_quality == "DEGRADED":
            # Binance healthy check: binance offline=0 AND drops=0 AND binance eps.min>100 AND queue_pct_peak<50
            if binance_offline == 0 and dropped_events == 0 and queue_pct_peak < 50:
                # Use eps_by_exchange.binance.min
                if binance_eps_min is not None and binance_eps_min > 100:
                    # check if DEGRADED cause was only other exchanges
                    # (In this context, if binance is healthy and we are degraded, 
                    # it must be due to max_offline (other) or drain_mode or reconnects)
                    post_quality = "GOOD"
                    reasons.append("Override: Binance Healthy -> GOOD")

        return {
            "window_start": window_json.get("window_start"),
            "original_quality": original_quality,
            "post_quality": post_quality,
            "is_partial": is_partial,
            "reasons": reasons,
            "binance_offline": binance_offline,
            "dropped_events": dropped_events
        }

    @staticmethod
    def aggregate_day(window_results: List[Dict]) -> Dict:
        """
        Aggregate window results into a day quality report.
        """
        total_windows = len(window_results)
        
        # Exclude PARTIAL from aggregation but keep for count
        active_windows = [w for w in window_results if not w['is_partial']]
        partial_windows = [w for w in window_results if w['is_partial']]
        
        bad_count = sum(1 for w in active_windows if w['post_quality'] == "BAD")
        degraded_count = sum(1 for w in active_windows if w['post_quality'] == "DEGRADED")
        good_count = sum(1 for w in active_windows if w['post_quality'] == "GOOD")
        
        total_drops = sum(w['dropped_events'] for w in window_results)
        binance_offline_total = sum(w['binance_offline'] for w in window_results)
        
        day_quality = "GOOD"
        
        # BAD day rules
        if bad_count >= 3 or total_drops > 100000 or binance_offline_total > 3600:
            day_quality = "BAD"
        # DEGRADED day rules
        elif (1 <= bad_count <= 2) or degraded_count >= 10 or binance_offline_total > 900:
            day_quality = "DEGRADED"
            
        # PARTIAL day check: present_windows < 80 AND partial_windows_count > 0
        # "PARTIAL day: partial_windows_count>0 ve (good+degraded+bad) < 80"
        # Since active_windows are (good+degraded+bad)
        if len(partial_windows) > 0 and len(active_windows) < 80:
            day_quality = "PARTIAL"
            
        return {
            "day_quality": day_quality,
            "version": POST_FILTER_VERSION,
            "stats": {
                "total_windows": total_windows,
                "good": good_count,
                "degraded": degraded_count,
                "bad": bad_count,
                "partial": len(partial_windows),
                "total_drops": total_drops,
                "binance_offline_total": binance_offline_total
            },
            "windows": window_results
        }
