"""
Auto-run Phase 1 setup without user input
"""
from setup_phase1 import Phase1Setup

if __name__ == "__main__":
    print("Starting Phase 1 setup (auto-confirmed)...")
    print("This will take 60-90 minutes to complete.")
    print("=" * 60)

    setup = Phase1Setup()
    setup.run_full_setup(auto_confirm=True)