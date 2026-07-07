"""
Unit tests for biosample_mapping validation module.

Tests the validation logic for biosample mapping CSV outputs.
"""
from nmdc_dp_utils.llm.biosample_mapping.validation import validate_biosample_mapping_csv


class TestValidateBiosampleMappingCSV:
    """Tests for validate_biosample_mapping_csv function."""

    def test_valid_mapping_passes(self):
        """Test that a valid mapping CSV passes validation."""
        biosample_csv = """id,name
nmdc:bsm-12-abc123,Sample 1
nmdc:bsm-12-def456,Sample 2"""

        raw_files_csv = """file_name
sample1_pos.raw
sample2_pos.raw
blank_01.raw"""

        yaml_content = """LCMS_POS:
  processedsamples:
    - PS_001: {}
    - PS_002: {}"""

        mapping_csv = """raw_data_identifier,biosample_id,biosample_name,match_confidence,processedsample_placeholder,material_processing_protocol_id
sample1_pos.raw,nmdc:bsm-12-abc123,Sample 1,high,PS_001,LCMS_POS
sample2_pos.raw,nmdc:bsm-12-def456,Sample 2,high,PS_002,LCMS_POS"""
        
        result = validate_biosample_mapping_csv(
            csv_content=mapping_csv,
            biosample_attributes_csv=biosample_csv,
            raw_files_csv=raw_files_csv,
            material_processing_yaml=yaml_content
        )
        
        assert result['valid'] is True
        assert len(result['errors']) == 0
        # blank_01.raw should be unmapped
        assert 'blank_01.raw' in result['unmapped_files']

    def test_empty_csv_fails(self):
        """Test that empty CSV fails validation."""
        biosample_csv = "id\ntest"
        raw_files_csv = "file_name\ntest.raw"
        yaml_content = "TEST: {}"
        empty_csv = "raw_data_identifier,biosample_id,biosample_name,match_confidence,processedsample_placeholder,material_processing_protocol_id\n"
        
        result = validate_biosample_mapping_csv(
            csv_content=empty_csv,
            biosample_attributes_csv=biosample_csv,
            raw_files_csv=raw_files_csv,
            material_processing_yaml=yaml_content
        )
        
        assert result['valid'] is False
        assert any('empty' in error.lower() for error in result['errors'])

    def test_missing_required_columns(self):
        """Test that missing required columns fails validation."""
        biosample_csv = "id\ntest"
        raw_files_csv = "file_name\ntest.raw"
        yaml_content = "TEST: {}"
        incomplete_csv = """raw_data_identifier,biosample_id
sample1_pos.raw,nmdc:bsm-12-abc123"""
        
        result = validate_biosample_mapping_csv(
            csv_content=incomplete_csv,
            biosample_attributes_csv=biosample_csv,
            raw_files_csv=raw_files_csv,
            material_processing_yaml=yaml_content
        )
        
        assert result['valid'] is False
        assert any('missing required columns' in error.lower() for error in result['errors'])

    def test_invalid_biosample_id_format(self):
        """Test that invalid biosample ID format fails validation."""
        biosample_csv = "id\ntest"
        raw_files_csv = "file_name\nsample1.raw"
        yaml_content = "LCMS:\n  processedsamples:\n    - PS: {}"
        
        csv_bad_id = """raw_data_identifier,biosample_id,biosample_name,match_confidence,processedsample_placeholder,material_processing_protocol_id
sample1.raw,invalid-id,Sample 1,high,PS,LCMS"""
        
        result = validate_biosample_mapping_csv(
            csv_content=csv_bad_id,
            biosample_attributes_csv=biosample_csv,
            raw_files_csv=raw_files_csv,
            material_processing_yaml=yaml_content
        )
        
        assert result['valid'] is False
        assert any('does not match nmdc format' in error.lower() for error in result['errors'])

    def test_biosample_id_not_in_attributes(self):
        """Test that biosample ID not in attributes CSV fails validation."""
        biosample_csv = "id,name\nnmdc:bsm-12-abc123,Sample A"
        raw_files_csv = "file_name\nsample1.raw"
        yaml_content = "LCMS:\n  processedsamples:\n    - PS: {}"
        
        csv_unknown_id = """raw_data_identifier,biosample_id,biosample_name,match_confidence,processedsample_placeholder,material_processing_protocol_id
sample1.raw,nmdc:bsm-99-zzz999,Unknown Sample,high,PS,LCMS"""
        
        result = validate_biosample_mapping_csv(
            csv_content=csv_unknown_id,
            biosample_attributes_csv=biosample_csv,
            raw_files_csv=raw_files_csv,
            material_processing_yaml=yaml_content
        )
        
        assert result['valid'] is False
        assert any('not found in biosample attributes' in error.lower() for error in result['errors'])

    def test_invalid_match_confidence(self):
        """Test that invalid match_confidence value fails validation."""
        biosample_csv = "id,name\nnmdc:bsm-12-abc123,Sample A"
        raw_files_csv = "file_name\nsample1.raw"
        yaml_content = "LCMS:\n  processedsamples:\n    - PS: {}"
        
        csv_bad_confidence = """raw_data_identifier,biosample_id,biosample_name,match_confidence,processedsample_placeholder,material_processing_protocol_id
sample1.raw,nmdc:bsm-12-abc123,Sample A,invalid,PS,LCMS"""
        
        result = validate_biosample_mapping_csv(
            csv_content=csv_bad_confidence,
            biosample_attributes_csv=biosample_csv,
            raw_files_csv=raw_files_csv,
            material_processing_yaml=yaml_content
        )
        
        assert result['valid'] is False
        assert any('match_confidence must be' in error.lower() for error in result['errors'])

    def test_processedsample_not_in_yaml(self):
        """Test that processed sample placeholder not in YAML fails validation."""
        biosample_csv = "id,name\nnmdc:bsm-12-abc123,Sample A"
        raw_files_csv = "file_name\nsample1.raw"
        yaml_content = "LCMS:\n  processedsamples:\n    - PS_001: {}"
        
        csv_bad_ps = """raw_data_identifier,biosample_id,biosample_name,match_confidence,processedsample_placeholder,material_processing_protocol_id
sample1.raw,nmdc:bsm-12-abc123,Sample A,high,PS_999,LCMS"""
        
        result = validate_biosample_mapping_csv(
            csv_content=csv_bad_ps,
            biosample_attributes_csv=biosample_csv,
            raw_files_csv=raw_files_csv,
            material_processing_yaml=yaml_content
        )
        
        assert result['valid'] is False
        assert any('not found in material processing yaml' in error.lower() for error in result['errors'])

    def test_protocol_id_not_in_yaml(self):
        """Test that protocol ID not in YAML fails validation."""
        biosample_csv = "id,name\nnmdc:bsm-12-abc123,Sample A"
        raw_files_csv = "file_name\nsample1.raw"
        yaml_content = "LCMS_POS:\n  processedsamples:\n    - PS: {}"
        
        csv_bad_protocol = """raw_data_identifier,biosample_id,biosample_name,match_confidence,processedsample_placeholder,material_processing_protocol_id
sample1.raw,nmdc:bsm-12-abc123,Sample A,high,PS,UNKNOWN_PROTOCOL"""
        
        result = validate_biosample_mapping_csv(
            csv_content=csv_bad_protocol,
            biosample_attributes_csv=biosample_csv,
            raw_files_csv=raw_files_csv,
            material_processing_yaml=yaml_content
        )
        
        assert result['valid'] is False
        assert any('not found in material processing yaml' in error.lower() for error in result['errors'])

    def test_empty_biosample_id_allowed(self):
        """Test that empty biosample_id is allowed (for QC/control samples)."""
        biosample_csv = "id,name\nnmdc:bsm-12-abc123,Sample A"
        raw_files_csv = "file_name\nsample1.raw\nblank_01.raw"
        yaml_content = "LCMS:\n  processedsamples:\n    - PS: {}"

        csv_with_empty_id = """raw_data_identifier,biosample_id,biosample_name,match_confidence,processedsample_placeholder,material_processing_protocol_id
sample1.raw,nmdc:bsm-12-abc123,Sample A,high,PS,LCMS
    blank_01.raw,,,,,"""

        result = validate_biosample_mapping_csv(
            csv_content=csv_with_empty_id,
            biosample_attributes_csv=biosample_csv,
            raw_files_csv=raw_files_csv,
            material_processing_yaml=yaml_content
        )

        # Should be valid - empty biosample_id is allowed
        assert result['valid'] is True

    def test_four_column_csv_valid_when_skip_flag(self):
        """When skip_material_processing=True, a 4-column CSV validates cleanly."""
        biosample_csv = "id,name\nnmdc:bsm-12-abc123,Sample A"
        raw_files_csv = "file_name\nsample1.raw"

        four_col_csv = """raw_data_identifier,biosample_id,biosample_name,match_confidence
sample1.raw,nmdc:bsm-12-abc123,Sample A,high"""

        result = validate_biosample_mapping_csv(
            csv_content=four_col_csv,
            biosample_attributes_csv=biosample_csv,
            raw_files_csv=raw_files_csv,
            material_processing_yaml="",
            skip_material_processing=True,
        )

        assert result['valid'] is True
        assert result['errors'] == []

    def test_four_column_csv_fails_without_flag(self):
        """Without the flag, a 4-column CSV is missing required columns."""
        biosample_csv = "id,name\nnmdc:bsm-12-abc123,Sample A"
        raw_files_csv = "file_name\nsample1.raw"

        four_col_csv = """raw_data_identifier,biosample_id,biosample_name,match_confidence
sample1.raw,nmdc:bsm-12-abc123,Sample A,high"""

        result = validate_biosample_mapping_csv(
            csv_content=four_col_csv,
            biosample_attributes_csv=biosample_csv,
            raw_files_csv=raw_files_csv,
            material_processing_yaml="LCMS:\n  processedsamples:\n    - PS: {}",
        )

        assert result['valid'] is False
        assert any('missing required columns' in error.lower() for error in result['errors'])

    def test_ps_and_protocol_checks_bypassed_when_flag(self):
        """The material-processing per-row checks are skipped when the flag is True."""
        biosample_csv = "id,name\nnmdc:bsm-12-abc123,Sample A"
        raw_files_csv = "file_name\nsample1.raw"

        # A row that would fail without the flag: no processedsample_placeholder
        # and no material_processing_protocol_id but match_confidence=high.
        four_col_csv = """raw_data_identifier,biosample_id,biosample_name,match_confidence
sample1.raw,nmdc:bsm-12-abc123,Sample A,high"""

        result = validate_biosample_mapping_csv(
            csv_content=four_col_csv,
            biosample_attributes_csv=biosample_csv,
            raw_files_csv=raw_files_csv,
            material_processing_yaml="",
            skip_material_processing=True,
        )

        assert result['valid'] is True
        assert not any('processedsample_placeholder' in err for err in result['errors'])
        assert not any('material_processing_protocol_id' in err for err in result['errors'])

    def test_six_column_csv_rejected_when_skip_flag(self):
        """Under the skip flag, extra MP columns are unexpected columns."""
        biosample_csv = "id,name\nnmdc:bsm-12-abc123,Sample A"
        raw_files_csv = "file_name\nsample1.raw"

        six_col_csv = """raw_data_identifier,biosample_id,biosample_name,match_confidence,processedsample_placeholder,material_processing_protocol_id
sample1.raw,nmdc:bsm-12-abc123,Sample A,high,PS,LCMS"""

        result = validate_biosample_mapping_csv(
            csv_content=six_col_csv,
            biosample_attributes_csv=biosample_csv,
            raw_files_csv=raw_files_csv,
            material_processing_yaml="",
            skip_material_processing=True,
        )

        assert result['valid'] is False
        assert any('unexpected columns' in err.lower() for err in result['errors'])
