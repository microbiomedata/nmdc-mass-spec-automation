import sys
from pathlib import Path

# Ensure project root is on sys.path so package `nmdc_dp_utils` is importable
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from nmdc_dp_utils.workflow_manager import NMDCWorkflowManager

def main():

    config_path = "/home/bmeluch/NMDC/nmdc-mass-spec-automation/studies/miesel_11_8bjf2432_nom/miesel_nom_config.json"
    manager = NMDCWorkflowManager(str(config_path))

    print(sys.path)
    print(PROJECT_ROOT)
    print(config_path)
    print(manager.workflow_path)

    logger = manager.logger
    logger.info(f"=== {manager.workflow_name.upper()} WORKFLOW ===")

    # Step 1: Create workflow structure
    logger.info("1. Creating workflow structure...")
    manager.create_workflow_structure()


    logger.info("generating material processing json")
    manager.generate_material_processing_metadata()

if __name__ == "__main__":
    main()