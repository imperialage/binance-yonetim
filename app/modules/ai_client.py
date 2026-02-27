"""AI explanation client – provider-agnostic interface."""

from __future__ import annotations

import abc

import httpx

from app.config import settings
from app.schemas.evaluation import AggregationResult, MarketSummary, RulesOutput
from app.utils.logging import get_logger

log = get_logger(__name__)


class AIClient(abc.ABC):
    @abc.abstractmethod
    async def explain(self, rules: RulesOutput, aggregation: AggregationResult, market: dict[str, MarketSummary]) -> str:
        ...


def _build_prompt(rules: RulesOutput, aggregation: AggregationResult, market: dict[str, MarketSummary]) -> str:
    """Build the prompt sent to AI."""
    tf_lines = []
    for tf in ["4h", "1h", "15m"]:
        ms = market.get(tf)
        ts = aggregation.timeframes.get(tf)
        if ms and ts:
            inds = ", ".join(f"{i.indicator}={i.signal}" for i in ts.indicators) or "yok"
            tf_lines.append(
                f"  {tf}: price={ms.last_price}, slope={ms.slope:+.2f}, "
                f"green/red={ms.green_candles}/{ms.red_candles}, sinyaller=[{inds}]"
            )

    prompt = f"""Sen bir kripto piyasa analisti asistansın. Kesin al/sat emri VERMEDEN aşağıdaki verilere göre
6 satırlık Türkçe özet üret. Şablon:

1) Genel Durum: {{decision}} ({{confidence}}/100)
2) Trend: 4H ... | 1H ...
3) Sinyal Özeti: hangi indikatör hangi tf'de ne dedi (kısa)
4) Senaryo A: yükseliş olursa ...
5) Senaryo B: düşüş olursa ...
6) Risk: volatilite/stop şart, "kesin al/sat" yok

Veriler:
- Symbol: {rules.symbol}
- Karar: {rules.decision} | Eğilim: {rules.bias} | Güven: {rules.confidence}/100 | Skor: {rules.score}
- Eşik: {rules.threshold} | Veto: {rules.veto_applied} ({rules.veto_reason or 'yok'})
- Nedenler: {'; '.join(rules.reasons) or 'yok'}
- Piyasa:
{chr(10).join(tf_lines)}

6 satırlık özeti Türkçe yaz. "Kesin al/sat" ifadesi kullanma."""
    return prompt


def _fallback_explanation(rules: RulesOutput, aggregation: AggregationResult, market: dict[str, MarketSummary]) -> str:
    """Template-based fallback when AI is unavailable."""
    m4h = market.get("4h")
    m1h = market.get("1h")

    trend_4h = "yukari" if (m4h and m4h.slope > 0) else "asagi"
    trend_1h = "yukari" if (m1h and m1h.slope > 0) else "asagi"

    tf_signals = []
    for tf, ts in aggregation.timeframes.items():
        for ind in ts.indicators:
            tf_signals.append(f"{ind.indicator}@{tf}={ind.signal}")

    veto_text = f" (Veto: {rules.veto_reason})" if rules.veto_applied else ""

    return (
        f"1) Genel Durum: {rules.decision} ({rules.confidence}/100){veto_text}\n"
        f"2) Trend: 4H {trend_4h} (slope={m4h.slope if m4h else 0:+.2f}) | 1H {trend_1h} (slope={m1h.slope if m1h else 0:+.2f})\n"
        f"3) Sinyal Ozeti: {', '.join(tf_signals) or 'sinyal yok'}\n"
        f"4) Senaryo A: Yukselis devam ederse mevcut bias ({rules.bias}) yonunde hareket.\n"
        f"5) Senaryo B: Dusus olursa bias degisebilir, stop/hedge degerlendir.\n"
        f"6) Risk: Skor={rules.score:.3f}, esik={rules.threshold}. Kesin al/sat degil, kendi analizinle dogrula."
    )


class DummyAIClient(AIClient):
    """Uses the fallback template – no external API call."""

    async def explain(self, rules: RulesOutput, aggregation: AggregationResult, market: dict[str, MarketSummary]) -> str:
        return _fallback_explanation(rules, aggregation, market)


class OpenAIClient(AIClient):
    """Calls an OpenAI-compatible chat completions endpoint."""

    def __init__(self, api_key: str, model: str, base_url: str):
        self.api_key = api_key
        self.model = model
        self.base_url = base_url.rstrip("/")

    async def explain(self, rules: RulesOutput, aggregation: AggregationResult, market: dict[str, MarketSummary]) -> str:
        prompt = _build_prompt(rules, aggregation, market)
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(
                    f"{self.base_url}/chat/completions",
                    headers={"Authorization": f"Bearer {self.api_key}"},
                    json={
                        "model": self.model,
                        "messages": [{"role": "user", "content": prompt}],
                        "temperature": 0.3,
                        "max_tokens": 500,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                return data["choices"][0]["message"]["content"].strip()
        except Exception as e:
            log.error("ai_call_failed", error=str(e))
            return _fallback_explanation(rules, aggregation, market)


def create_ai_client() -> AIClient:
    if settings.ai_provider == "openai" and settings.ai_api_key:
        return OpenAIClient(
            api_key=settings.ai_api_key,
            model=settings.ai_model,
            base_url=settings.ai_base_url,
        )
    return DummyAIClient()
