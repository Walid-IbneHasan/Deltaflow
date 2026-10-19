"""Postgres's numeric arithmetic where Python's would differ.

Python's Decimal adds, subtracts and multiplies exactly as Postgres's numeric
does, display scale included (the scale of a sum is the larger operand's, of
a product the sum of both), given enough precision: `EXACT` traps rounding.
Division and rounding differ, and are reproduced here from numeric.c:

- `numeric_div`: the result scale from select_div_scale, rounded half away
  from zero;
- `round_half_away`: round(numeric, s) and numeric(p, s) typmods.
"""

from __future__ import annotations

import decimal
from decimal import Decimal
from fractions import Fraction

#: exact arithmetic: a result that cannot be held exactly raises
EXACT = decimal.Context(
    prec=1000, traps=[decimal.Inexact, decimal.Overflow, decimal.InvalidOperation]
)

#: from numeric.c: significant digits a division keeps at least, decimal
#: digits per base-10000 digit, and the largest display scale
MIN_SIG_DIGITS, DEC_DIGITS, MAX_DISPLAY_SCALE = 16, 4, 1000


def scale_of(value: int | Decimal) -> int:
    """A number's display scale: digits after the decimal point."""
    if isinstance(value, int):
        return 0
    exponent = value.as_tuple().exponent
    assert isinstance(exponent, int), f"not a finite number: {value!r}"
    return max(0, -exponent)


def positive_zero(value: Decimal) -> Decimal:
    """Postgres's numeric has no negative zero."""
    return value.copy_abs() if value.is_zero() else value


def _weight_and_first(value: int | Decimal) -> tuple[int, int]:
    """Postgres's layout of |value| in base 10000: the weight of its first
    non-zero digit, and that digit (0, 0 for zero)."""
    if value == 0:
        return 0, 0
    magnitude = abs(Fraction(value))
    # copy_abs, not abs(): abs rounds to the context's 28 digits
    weight = Decimal(value).copy_abs().adjusted() // DEC_DIGITS
    return weight, int(magnitude / Fraction(10) ** (DEC_DIGITS * weight))


def numeric_div(
    a: int | Decimal, b: int | Decimal, *, scale_a: int | None = None, scale_b: int | None = None
) -> Decimal:
    """`a / b` as Postgres's numeric division computes it.

    The result scale is select_div_scale's: enough decimals for 16
    significant digits (from an estimate of the quotient's weight), no fewer
    than either operand's display scale, and at most 1000. The exact
    quotient is rounded half away from zero (div_var, round = true). A
    caller that knows an operand's display scale better than its Decimal
    does passes it (an average's sum). Raises ZeroDivisionError."""
    if b == 0:
        raise ZeroDivisionError("division by zero")
    weight1, first1 = _weight_and_first(a)
    weight2, first2 = _weight_and_first(b)
    qweight = weight1 - weight2 - (1 if first1 <= first2 else 0)
    s1 = scale_of(a) if scale_a is None else scale_a
    s2 = scale_of(b) if scale_b is None else scale_b
    rscale = min(max(MIN_SIG_DIGITS - qweight * DEC_DIGITS, s1, s2, 0), MAX_DISPLAY_SCALE)
    return _rounded(Fraction(a) / Fraction(b), rscale)


def round_half_away(value: int | Decimal, scale: int) -> Decimal:
    """round(value, scale): half away from zero, with `scale` decimals (none
    if negative: round(1234.5, -2) is 1200)."""
    result = _rounded(Fraction(value), scale)
    return result if scale >= 0 else result.quantize(Decimal(1), context=EXACT)


def _rounded(q: Fraction, scale: int) -> Decimal:
    scaled = q * Fraction(10) ** scale
    magnitude = (2 * abs(scaled.numerator) + scaled.denominator) // (2 * scaled.denominator)
    sign = 1 if scaled < 0 and magnitude else 0
    return Decimal((sign, tuple(int(c) for c in str(magnitude)), -scale))
