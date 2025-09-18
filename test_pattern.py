"""
Test pattern detection logic to verify correctness
"""

from scanner.pattern_detector import PatternDetector, Candle

# Create detector
detector = PatternDetector(marubozu_threshold=0.8, doji_threshold=0.25)

# Test Case 1: RELIANCE from results
print("=" * 60)
print("TEST 1: RELIANCE Pattern")
print("=" * 60)

marubozu = Candle(
    date="2025-08-19",
    open=1390.0,
    high=1421.0,
    low=1389.1,
    close=1420.1,
    volume=14384719.0
)

doji = Candle(
    date="2025-08-20",
    open=1413.0,
    high=1424.9,
    low=1410.0,
    close=1413.0,
    volume=8725641.0
)

print(f"Marubozu: Open={marubozu.open}, Close={marubozu.close}, High={marubozu.high}, Low={marubozu.low}")
print(f"  Body = |{marubozu.close} - {marubozu.open}| = {marubozu.body:.1f}")
print(f"  Range = {marubozu.high} - {marubozu.low} = {marubozu.range:.1f}")
print(f"  Body% = {marubozu.body_pct:.2f}%")
print(f"  Direction: {'BULLISH' if marubozu.is_bullish else 'BEARISH'}")
print()

print(f"Doji: Open={doji.open}, Close={doji.close}, High={doji.high}, Low={doji.low}")
print(f"  Body = |{doji.close} - {doji.open}| = {doji.body:.1f}")
print(f"  Range = {doji.high} - {doji.low} = {doji.range:.1f}")
print(f"  Body% = {doji.body_pct:.2f}%")
print()

print("Pattern Checks:")
print(f"1. Is Marubozu? Body% ({marubozu.body_pct:.2f}%) >= 80%? {marubozu.body_pct >= 80}")
print(f"2. Is Doji? Body% ({doji.body_pct:.2f}%) < 25%? {doji.body_pct < 25}")
print(f"3. Doji high ({doji.high}) > Marubozu high ({marubozu.high})? {doji.high > marubozu.high}")
print()

print("4. Doji close inside Marubozu body?")
if marubozu.is_bullish:
    print(f"   Bullish Marubozu body: {marubozu.open} to {marubozu.close}")
    print(f"   Required: {marubozu.open} < {doji.close} < {marubozu.close}")
    inside = marubozu.open < doji.close < marubozu.close
    print(f"   Result: {inside}")
else:
    print(f"   Bearish Marubozu body: {marubozu.close} to {marubozu.open}")
    print(f"   Required: {marubozu.close} < {doji.close} < {marubozu.open}")
    inside = marubozu.close < doji.close < marubozu.open
    print(f"   Result: {inside}")

print()
matched, details = detector.matches_marubozu_doji(marubozu, doji)
print(f"Pattern Match: {matched}")
if matched:
    print(f"Details: {details}")

print("\n" + "=" * 60)
print("TEST 2: Ideal Pattern Example")
print("=" * 60)

# Create an ideal bullish Marubozu->Doji pattern
ideal_marubozu = Candle(
    date="2025-01-01",
    open=100.0,
    high=110.0,
    low=99.0,
    close=109.0,  # Bullish, body = 9, range = 11, body% = 81.8%
    volume=1000
)

ideal_doji = Candle(
    date="2025-01-02",
    open=108.0,
    high=111.0,  # Breaks above marubozu high
    low=107.0,
    close=108.5,  # Closes inside marubozu body (100-109)
    volume=1000
)

print(f"Marubozu: Open={ideal_marubozu.open}, Close={ideal_marubozu.close}")
print(f"  Body% = {ideal_marubozu.body_pct:.2f}%")
print(f"Doji: Open={ideal_doji.open}, Close={ideal_doji.close}, High={ideal_doji.high}")
print(f"  Body% = {ideal_doji.body_pct:.2f}%")
print(f"  Doji high ({ideal_doji.high}) > Marubozu high ({ideal_marubozu.high})? {ideal_doji.high > ideal_marubozu.high}")
print(f"  Doji close ({ideal_doji.close}) inside Marubozu body ({ideal_marubozu.open}-{ideal_marubozu.close})? {ideal_marubozu.open < ideal_doji.close < ideal_marubozu.close}")

matched2, details2 = detector.matches_marubozu_doji(ideal_marubozu, ideal_doji)
print(f"\nPattern Match: {matched2}")
if matched2:
    print(f"Details: {details2}")

print("\n" + "=" * 60)
print("TEST 3: Failed Pattern (Doji closes outside body)")
print("=" * 60)

bad_doji = Candle(
    date="2025-01-02",
    open=108.0,
    high=111.0,  # Breaks above marubozu high
    low=107.0,
    close=110.0,  # Closes OUTSIDE marubozu body (above 109)
    volume=1000
)

print(f"Doji close ({bad_doji.close}) outside Marubozu body ({ideal_marubozu.open}-{ideal_marubozu.close})")
print(f"Should NOT match pattern")

matched3, details3 = detector.matches_marubozu_doji(ideal_marubozu, bad_doji)
print(f"Pattern Match: {matched3} (Should be False)")