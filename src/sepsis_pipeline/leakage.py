"""Fixed patients for the truncation leakage test (docs/PROJECT.md §9).

They are always included in `--sample` loads so the test can run on the sample database.
Chosen from the Stage 2 profile to cover both hospitals and both outcomes; every septic
patient's label first turns 1 well after LEAKAGE_CUTOFF_HOUR (at ICULOS 44-61), so the
truncation happens before onset; p012375's record starts at ICULOS 2 (not 1).
"""

LEAKAGE_TEST_PATIENTS: tuple[str, ...] = (
    "p000269",  # A, non-septic
    "p004092",  # A, non-septic
    "p002130",  # A, septic, first positive ICULOS 44
    "p003152",  # A, septic, first positive ICULOS 56
    "p012375",  # A, septic, first positive ICULOS 55, record starts at ICULOS 2
    "p105265",  # B, non-septic
    "p105486",  # B, non-septic
    "p104106",  # B, septic, first positive ICULOS 61
    "p104754",  # B, septic, first positive ICULOS 52
)

# ICULOS after which the test drops these patients' rows.
LEAKAGE_CUTOFF_HOUR = 30
