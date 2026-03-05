"""
risk/portfolio_controller.py — Portfolio-level risk gate.

Pure-function design: no global state, no I/O.  All inputs are passed
explicitly; returns a PortfolioDecision that the engine acts on.

Rules (evaluated in priority order):
    1. Max concurrent positions  → BLOCK
    2. Gross exposure cap        → BLOCK
    3. Per-sector cap            → BLOCK
    4. Low-liquidity concurrency → BLOCK
    5. Correlated-cluster size   → ALLOW_WITH_MULTIPLIER

Sector mapping is a static lookup; extend SECTOR_MAP as needed.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from math import floor
from typing import Any, Dict, List, Optional

from config.settings import CONFIG

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Sector mapping (add tickers as the universe grows)
# ─────────────────────────────────────────────────────────────────────────────

SECTOR_MAP: Dict[str, str] = {
    # Semiconductors
    "NVDA": "semiconductors",
    "AMD": "semiconductors",
    "INTC": "semiconductors",
    "MU": "semiconductors",
    "SOXL": "semiconductors",
    # EV / Auto
    "TSLA": "ev",
    "RIVN": "ev",
    "LCID": "ev",
    "NIO": "ev",
    "XPEV": "ev",
    "LI": "ev",
    # Crypto / Blockchain
    "MARA": "crypto",
    "RIOT": "crypto",
    "COIN": "crypto",
    "MSTR": "crypto",
    # Meme / Short-squeeze
    "AMC": "meme",
    "GME": "meme",
    "BBBY": "meme",
    "ATER": "meme",
    "PROG": "meme",
    # Chinese ADRs
    "FUTU": "china_adr",
    "TIGR": "china_adr",
    # Fintech
    "SOFI": "fintech",
    "HOOD": "fintech",
    "UPST": "fintech",
    "AFRM": "fintech",
    # Big Tech
    "AAPL": "bigtech",
    "MSFT": "bigtech",
    "GOOGL": "bigtech",
    "AMZN": "bigtech",
    "META": "bigtech",
    "NFLX": "bigtech",
    # Cannabis / retail meme
    "SNDL": "cannabis",
    "CLOV": "biotech",
}

_DEFAULT_SECTOR = "other"


def _sector(ticker: str) -> str:
    return SECTOR_MAP.get(ticker.upper(), _DEFAULT_SECTOR)


# ─────────────────────────────────────────────────────────────────────────────
# Public API types
# ─────────────────────────────────────────────────────────────────────────────

ALLOW = "ALLOW"
ALLOW_WITH_MULTIPLIER = "ALLOW_WITH_MULTIPLIER"
BLOCK = "BLOCK"


@dataclass(frozen=True)
class PortfolioDecision:
    """Immutable decision returned by evaluate()."""

    action: str = ALLOW  # ALLOW | ALLOW_WITH_MULTIPLIER | BLOCK
    reason: str = ""
    multiplier: float = 1.0
    qty_final: int = 0
    cap_reason: str = ""
    cap_values: str = ""


@dataclass(frozen=True)
class OpenPosition:
    """Snapshot of a single open position passed in from RiskManager."""

    ticker: str
    notional: float  # shares * entry_price
    sector: str
    dollar_volume: float  # daily dollar volume at the time of entry


@dataclass(frozen=True)
class RiskCheckResult:
    allowed: bool
    suggested_shares: int = 0
    reason: str = ""
    notional: float = 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Core evaluation
# ─────────────────────────────────────────────────────────────────────────────


def evaluate(
    ticker: str,
    planned_qty: int,
    planned_notional: float,
    open_positions: List[OpenPosition],
    regime: str = "",
    dollar_volume: float = 0.0,
    entry_price: float = 0.0,
    recent_volume: float = 0.0,
) -> PortfolioDecision:
    """
    Evaluate whether a new position should be opened.

    Parameters
    ----------
    ticker            : Ticker being considered for entry
    planned_qty       : Shares about to be submitted
    planned_notional  : planned_qty * entry_price (approximate)
    open_positions    : Current open book as OpenPosition objects
    regime            : Current regime string (e.g. "TREND", "CHOP")
    dollar_volume     : Daily dollar volume for the candidate ticker

    Returns
    -------
    PortfolioDecision with action ALLOW | ALLOW_WITH_MULTIPLIER | BLOCK
    """
    cfg = CONFIG.portfolio
    equity = CONFIG.risk.account_size

    # Normalize inputs
    qty_base = max(1, int(planned_qty))
    px = float(entry_price)
    if px <= 0 and qty_base > 0 and planned_notional > 0:
        px = float(planned_notional) / float(qty_base)
    if px <= 0:
        px = 1.0

    # ── Deterministic cap formula ────────────────────────────────────────────
    qty_cap1 = floor((equity * cfg.max_notional_per_trade_pct) / px)
    qty_cap2 = (
        floor(cfg.max_trade_notional / px) if cfg.max_trade_notional > 0 else qty_base
    )
    qty_cap3 = (
        floor((dollar_volume * cfg.liquidity_notional_cap_pct_of_dvol) / px)
        if dollar_volume > 0 and cfg.liquidity_notional_cap_pct_of_dvol > 0
        else qty_base
    )
    qty_cap4 = (
        floor(recent_volume * cfg.liquidity_share_cap_pct_of_volume)
        if recent_volume > 0 and cfg.liquidity_share_cap_pct_of_volume > 0
        else qty_base
    )

    # Correlation/sector multiplier remains deterministic and explicit.
    new_sector = _sector(ticker)
    same_sector_open = [p for p in open_positions if p.sector == new_sector]
    qty_corr = qty_base
    corr_mult = 1.0
    if same_sector_open and cfg.correlated_size_multiplier < 1.0:
        corr_mult = cfg.correlated_size_multiplier
        qty_corr = floor(qty_base * corr_mult)

    qty_final = max(1, min(qty_base, qty_cap1, qty_cap2, qty_cap3, qty_cap4, qty_corr))
    cap_parts = []
    if qty_final < qty_base:
        if qty_final == qty_cap1:
            cap_parts.append("max_notional_per_trade_pct")
        if qty_final == qty_cap2:
            cap_parts.append("max_trade_notional")
        if qty_final == qty_cap3:
            cap_parts.append("liquidity_notional_cap_pct_of_dvol")
        if qty_final == qty_cap4:
            cap_parts.append("liquidity_share_cap_pct_of_volume")
        if qty_final == qty_corr:
            cap_parts.append("correlated_size_multiplier")

    # ── Rule 1: max concurrent positions ─────────────────────────────────────
    if len(open_positions) >= cfg.max_concurrent_positions:
        return PortfolioDecision(
            action=BLOCK,
            reason=(
                f"max_concurrent_positions reached "
                f"({len(open_positions)}/{cfg.max_concurrent_positions})"
            ),
            qty_final=0,
            cap_reason="max_concurrent_positions",
        )

    # ── Rule 2: gross exposure cap ────────────────────────────────────────────
    gross_open = sum(p.notional for p in open_positions)
    gross_limit = equity * min(cfg.max_gross_exposure_pct, cfg.leverage_cap)
    net_limit = equity * cfg.max_net_exposure_pct

    # Deterministically reduce qty to fit gross/net caps.
    if px > 0:
        fit_gross_qty = floor(max(0.0, gross_limit - gross_open) / px)
        fit_net_qty = floor(max(0.0, net_limit - gross_open) / px)  # long-only proxy
        fit_qty = max(0, min(fit_gross_qty, fit_net_qty))
        if fit_qty <= 0:
            return PortfolioDecision(
                action=BLOCK,
                reason=(
                    f"gross_exposure {gross_open/equity if equity>0 else 0.0:.1%} already at/above cap "
                    f"{cfg.max_gross_exposure_pct:.1%}"
                ),
                qty_final=0,
                cap_reason="max_gross_exposure_pct",
            )
        if fit_qty < qty_final:
            qty_final = max(1, fit_qty)
            cap_parts.append("max_gross_exposure_pct")

    gross_exposure = gross_open + qty_final * px
    exposure_pct = gross_exposure / equity if equity > 0 else 0.0
    if exposure_pct > cfg.max_gross_exposure_pct + 1e-12:
        return PortfolioDecision(
            action=BLOCK,
            reason=(
                f"gross_exposure {exposure_pct:.1%} > cap "
                f"{cfg.max_gross_exposure_pct:.1%}"
            ),
            qty_final=0,
            cap_reason="max_gross_exposure_pct",
        )

    # ── Rule 3: per-sector cap ────────────────────────────────────────────────
    sector_count = sum(1 for p in open_positions if p.sector == new_sector)
    if sector_count >= cfg.max_per_sector:
        return PortfolioDecision(
            action=BLOCK,
            reason=(
                f"sector cap: {new_sector} already has "
                f"{sector_count}/{cfg.max_per_sector} positions"
            ),
            qty_final=0,
            cap_reason="max_per_sector",
        )

    # ── Rule 4: low-liquidity concurrency ─────────────────────────────────────
    if dollar_volume > 0 and dollar_volume < cfg.low_liquidity_dvol_threshold:
        low_liq_count = sum(
            1
            for p in open_positions
            if p.dollar_volume > 0
            and p.dollar_volume < cfg.low_liquidity_dvol_threshold
        )
        if low_liq_count >= cfg.max_low_liquidity_concurrent:
            return PortfolioDecision(
                action=BLOCK,
                reason=(
                    f"low_liquidity concurrency: already {low_liq_count} "
                    f"low-dvol positions open (dvol={dollar_volume:,.0f})"
                ),
                qty_final=0,
                cap_reason="max_low_liquidity_concurrent",
            )

    # Absolute min/max notional checks after all caps
    final_notional = qty_final * px
    if final_notional < cfg.min_trade_notional:
        return PortfolioDecision(
            action=BLOCK,
            reason=(
                f"min_trade_notional not met: ${final_notional:.2f} < ${cfg.min_trade_notional:.2f}"
            ),
            qty_final=0,
            cap_reason="min_trade_notional",
        )
    if cfg.max_trade_notional > 0 and final_notional > cfg.max_trade_notional + 1e-9:
        return PortfolioDecision(
            action=BLOCK,
            reason=(
                f"max_trade_notional exceeded: ${final_notional:.2f} > ${cfg.max_trade_notional:.2f}"
            ),
            qty_final=0,
            cap_reason="max_trade_notional",
        )

    cap_reason = "|".join(cap_parts)
    cap_values = (
        f"qty_base={qty_base};qty_cap1={qty_cap1};qty_cap2={qty_cap2};"
        f"qty_cap3={qty_cap3};qty_cap4={qty_cap4};qty_corr={qty_corr};"
        f"qty_final={qty_final};equity={equity:.2f};px={px:.4f};dvol={dollar_volume:.2f};"
        f"gross_open={gross_open:.2f};gross_limit={gross_limit:.2f}"
    )
    if cap_reason:
        corr_applied = "correlated_size_multiplier" in cap_parts
        return PortfolioDecision(
            action=ALLOW_WITH_MULTIPLIER if corr_applied else ALLOW,
            reason=f"size capped by {cap_reason}",
            multiplier=(qty_final / qty_base) if corr_applied and qty_base > 0 else 1.0,
            qty_final=qty_final,
            cap_reason=cap_reason,
            cap_values=cap_values,
        )

    return PortfolioDecision(
        action=ALLOW,
        reason="all portfolio gates clear",
        multiplier=1.0,
        qty_final=qty_final,
        cap_reason="",
        cap_values=cap_values,
    )


class PortfolioController:
    """
    Class-based portfolio gate API.

    Keeps compatibility with existing pure evaluate() function while providing
    an explicit object-oriented entry point for external integrations.
    """

    def __init__(self) -> None:
        self.config = CONFIG

    def evaluate_entry(
        self,
        ticker: str,
        entry_price: float,
        stop_loss: float,
        current_portfolio: Dict[str, Any],
        daily_stats: Dict[str, Any],
        ticker_dvol: float,
        sector: Optional[str] = None,
    ) -> RiskCheckResult:
        """
        Calculates position size and validates against portfolio constraints.
        """
        # 1. HARD GATE: Daily loss cap
        if (
            daily_stats.get("total_loss_pct", 0.0)
            >= self.config.risk.daily_loss_cap_pct
        ):
            return RiskCheckResult(False, reason="Daily loss cap reached.")

        # 2. HARD GATE: Max concurrent positions
        if len(current_portfolio) >= self.config.portfolio.max_concurrent_positions:
            return RiskCheckResult(False, reason="Max concurrent positions reached.")

        # 3. Fixed-R risk amount
        equity = self.config.risk.account_size
        risk_per_trade_dollars = equity * self.config.risk.risk_per_trade_pct

        # 4. Share sizing by risk-per-share
        risk_per_share = abs(entry_price - stop_loss)
        if risk_per_share <= 0:
            return RiskCheckResult(
                False,
                reason="Invalid stop loss (zero risk per share).",
            )

        suggested_shares = math.floor(risk_per_trade_dollars / risk_per_share)
        notional_value = suggested_shares * entry_price

        # 5. Max notional per trade cap
        max_notional = equity * self.config.portfolio.max_notional_per_trade_pct
        if notional_value > max_notional:
            suggested_shares = math.floor(max_notional / max(entry_price, 1e-9))
            notional_value = suggested_shares * entry_price
            logger.info("[%s] Sizing capped by max_notional_per_trade_pct", ticker)

        # 6. Gross exposure cap
        current_gross = sum(
            float((pos or {}).get("notional", 0.0) or 0.0)
            for pos in current_portfolio.values()
        )
        gross_cap = equity * self.config.portfolio.max_gross_exposure_pct
        if (current_gross + notional_value) > gross_cap:
            return RiskCheckResult(
                False, reason="Trade exceeds max_gross_exposure_pct."
            )

        # 7. Liquidity gate: notional <= pct of daily dollar volume
        liquidity_cap = (
            ticker_dvol * self.config.portfolio.liquidity_notional_cap_pct_of_dvol
        )
        if liquidity_cap > 0 and notional_value > liquidity_cap:
            suggested_shares = math.floor(liquidity_cap / max(entry_price, 1e-9))
            notional_value = suggested_shares * entry_price
            logger.info("[%s] Sizing capped by liquidity (DVOL)", ticker)

        # 8. Sector/correlation limits
        candidate_sector = sector or _sector(ticker)
        sector_count = sum(
            1
            for pos in current_portfolio.values()
            if str((pos or {}).get("sector", "") or "").lower()
            == str(candidate_sector or "").lower()
        )
        if sector_count >= self.config.portfolio.max_per_sector:
            return RiskCheckResult(
                False,
                reason=f"Max positions in sector {candidate_sector} reached.",
            )

        if sector_count > 0:
            suggested_shares = math.floor(
                suggested_shares * self.config.portfolio.correlated_size_multiplier
            )
            notional_value = suggested_shares * entry_price
            logger.info("[%s] Sizing reduced due to existing sector exposure", ticker)

        # 9. Absolute notional bounds
        if notional_value < self.config.portfolio.min_trade_notional:
            return RiskCheckResult(
                False,
                reason=f"Notional ${notional_value:.2f} below minimum.",
            )

        if notional_value > self.config.portfolio.max_trade_notional:
            suggested_shares = math.floor(
                self.config.portfolio.max_trade_notional / max(entry_price, 1e-9)
            )
            notional_value = suggested_shares * entry_price

        # 10. Safe mode extension hook
        if self.config.safe_mode.enabled:
            pass

        return RiskCheckResult(
            allowed=True,
            suggested_shares=max(0, int(suggested_shares)),
            notional=float(notional_value),
            reason="Validated",
        )


portfolio_controller = PortfolioController()
