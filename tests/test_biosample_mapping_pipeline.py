"""
Unit tests for biosample_mapping pipeline module.

Tests the LLM-based code generation pipeline for biosample mapping.
"""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
import pytest

from nmdc_dp_utils.llm.biosample_mapping.pipeline import (
    get_llm_generated_script,
    validate_and_fix_script,
    add_study_data_to_conversation
)


class DummyConversation:
    """Mock conversation manager."""
    def __init__(self):
        self.messages = []
    
    def add_message(self, role, content):
        self.messages.append({"role": role, "content": content})




class TestGetLLMGeneratedScript:
    """Tests for get_llm_generated_script function."""

    def test_generates_script_with_correct_prompt(self, tmp_path):
        """Test that script generation sends correct prompt to LLM."""
        # Create temporary files
        biosample_file = tmp_path / "biosamples.csv"
        biosample_file.write_text("id,name\nnmdc:bsm-12-abc123,Sample A")
        
        files_file = tmp_path / "files.csv"
        files_file.write_text("file_name\nsample_a.raw")
        
        yaml_file = tmp_path / "protocol.yaml"
        yaml_file.write_text("TEST: {}")
        
        output_file = tmp_path / "output.csv"
        
        # Mock LLM client and conversation
        conversation = DummyConversation()
        client = SimpleNamespace(get_response=AsyncMock(return_value="print('test script')"))
        
        # Call function
        result = asyncio.run(get_llm_generated_script(
            llm_client=client,
            conversation_obj=conversation,
            biosample_path=str(biosample_file),
            files_path=str(files_file),
            yaml_path=str(yaml_file),
            output_path=str(output_file)
        ))
        
        # Verify LLM was called
        assert client.get_response.await_count == 1
        assert result == "print('test script')"
        
        # Verify correct prompt was added to conversation
        assert len(conversation.messages) == 1
        assert conversation.messages[0]['role'] == 'user'
        assert 'file_name' in conversation.messages[0]['content']
        assert str(biosample_file) in conversation.messages[0]['content']
        assert str(files_file) in conversation.messages[0]['content']
        assert str(yaml_file) in conversation.messages[0]['content']
        assert str(output_file) in conversation.messages[0]['content']

    def test_handles_alternate_column_name(self, tmp_path):
        """Test that function detects raw_data_file_name column."""
        # Create files with alternate column name
        files_file = tmp_path / "files.csv"
        files_file.write_text("raw_data_file_name\nsample_a.raw")
        
        biosample_file = tmp_path / "biosamples.csv"
        biosample_file.write_text("id\nnmdc:bsm-12-abc123")
        
        yaml_file = tmp_path / "protocol.yaml"
        yaml_file.write_text("TEST: {}")
        
        conversation = DummyConversation()
        client = SimpleNamespace(get_response=AsyncMock(return_value="script code"))
        
        asyncio.run(get_llm_generated_script(
            llm_client=client,
            conversation_obj=conversation,
            biosample_path=str(biosample_file),
            files_path=str(files_file),
            yaml_path=str(yaml_file),
            output_path=str(tmp_path / "output.csv")
        ))
        
        # Check that raw_data_file_name was detected
        assert 'raw_data_file_name' in conversation.messages[0]['content']

    def test_uses_timeout_parameter(self, tmp_path):
        """Test that timeout is passed to LLM client."""
        files_file = tmp_path / "files.csv"
        files_file.write_text("file_name\ntest.raw")
        biosample_file = tmp_path / "biosamples.csv"
        biosample_file.write_text("id\ntest")
        yaml_file = tmp_path / "protocol.yaml"
        yaml_file.write_text("TEST: {}")
        
        conversation = DummyConversation()
        client = SimpleNamespace(get_response=AsyncMock(return_value="script"))
        
        asyncio.run(get_llm_generated_script(
            llm_client=client,
            conversation_obj=conversation,
            biosample_path=str(biosample_file),
            files_path=str(files_file),
            yaml_path=str(yaml_file),
            output_path=str(tmp_path / "output.csv")
        ))
        
        # Verify timeout was passed
        call_kwargs = client.get_response.call_args[1]
        assert call_kwargs['timeout_seconds'] == 300


class TestValidateAndFixScript:
    """Tests for validate_and_fix_script function."""

    def test_validation_passes_on_first_try(self, tmp_path):
        """Test successful validation on first iteration."""
        # Create input files
        biosample_file = tmp_path / "biosamples.csv"
        biosample_file.write_text("id,name\nnmdc:bsm-12-abc123,Sample A")
        
        files_file = tmp_path / "files.csv"
        files_file.write_text("file_name\nsample_a.raw")
        
        yaml_file = tmp_path / "protocol.yaml"
        yaml_file.write_text("LCMS:\n  processedsamples:\n    - PS_001: {}")
        
        # Create valid output CSV
        output_file = tmp_path / "output.csv"
        output_file.write_text(
            "raw_data_identifier,biosample_id,biosample_name,match_confidence,"
            "processedsample_placeholder,material_processing_protocol_id\n"
            "sample_a.raw,nmdc:bsm-12-abc123,Sample A,high,PS_001,LCMS"
        )
        
        # Create script file
        script_file = tmp_path / "script.py"
        script_file.write_text("print('success')")
        
        conversation = DummyConversation()
        client = SimpleNamespace(get_response=AsyncMock())
        
        # Mock subprocess to simulate successful script execution
        with patch('nmdc_dp_utils.llm.biosample_mapping.pipeline.subprocess.run') as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout="", stderr="")
            
            result = asyncio.run(validate_and_fix_script(
                llm_client=client,
                conversation_obj=conversation,
                script_path=str(script_file),
                output_path=str(output_file),
                biosample_path=str(biosample_file),
                files_path=str(files_file),
                yaml_path=str(yaml_file),
                max_iterations=3
            ))
        
        assert result is True
        # LLM should not be called for fixes
        assert client.get_response.await_count == 0

    def test_script_execution_failure_triggers_fix(self, tmp_path):
        """Test that script execution failure triggers LLM fix."""
        biosample_file = tmp_path / "biosamples.csv"
        biosample_file.write_text("id\ntest")
        files_file = tmp_path / "files.csv"
        files_file.write_text("file_name\ntest.raw")
        yaml_file = tmp_path / "protocol.yaml"
        yaml_file.write_text("TEST:\n  processedsamples:\n    - PS: {}")
        script_file = tmp_path / "script.py"
        script_file.write_text("broken script")
        output_file = tmp_path / "output.csv"
        
        conversation = DummyConversation()
        client = SimpleNamespace(get_response=AsyncMock(return_value="print('fixed')"))
        
        call_count = [0]
        
        # Create valid output for second run
        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                # First execution fails
                return Mock(returncode=1, stdout="", stderr="SyntaxError: bad code")
            else:
                # Subsequent executions succeed
                # Create valid output file
                output_file.write_text(
                    "raw_data_identifier,biosample_id,biosample_name,match_confidence,"
                    "processedsample_placeholder,material_processing_protocol_id\n"
                    "test.raw,,,medium,PS,TEST"
                )
                return Mock(returncode=0, stdout="", stderr="")
        
        with patch('nmdc_dp_utils.llm.biosample_mapping.pipeline.subprocess.run') as mock_run:
            mock_run.side_effect = side_effect
            
            result = asyncio.run(validate_and_fix_script(
                llm_client=client,
                conversation_obj=conversation,
                script_path=str(script_file),
                output_path=str(output_file),
                biosample_path=str(biosample_file),
                files_path=str(files_file),
                yaml_path=str(yaml_file),
                max_iterations=3
            ))
        
        # Should eventually succeed after fix
        assert result is True
        # LLM should be called for fix
        assert client.get_response.await_count == 1

    def test_max_iterations_exceeded_returns_false(self, tmp_path):
        """Test that function returns False after max iterations."""
        biosample_file = tmp_path / "biosamples.csv"
        biosample_file.write_text("id\ntest")
        files_file = tmp_path / "files.csv"
        files_file.write_text("file_name\ntest.raw")
        yaml_file = tmp_path / "protocol.yaml"
        yaml_file.write_text("TEST: {}")
        script_file = tmp_path / "script.py"
        script_file.write_text("broken")
        output_file = tmp_path / "output.csv"
        
        conversation = DummyConversation()
        client = SimpleNamespace(get_response=AsyncMock(return_value="still broken"))
        
        # Always fail
        with patch('nmdc_dp_utils.llm.biosample_mapping.pipeline.subprocess.run') as mock_run:
            mock_run.return_value = Mock(returncode=1, stdout="", stderr="Error")
            
            result = asyncio.run(validate_and_fix_script(
                llm_client=client,
                conversation_obj=conversation,
                script_path=str(script_file),
                output_path=str(output_file),
                biosample_path=str(biosample_file),
                files_path=str(files_file),
                yaml_path=str(yaml_file),
                max_iterations=2
            ))
        
        assert result is False

    def test_cleans_markdown_from_llm_response(self, tmp_path):
        """Test that markdown code blocks are cleaned from LLM response."""
        biosample_file = tmp_path / "biosamples.csv"
        biosample_file.write_text("id\ntest")
        files_file = tmp_path / "files.csv"
        files_file.write_text("file_name\ntest.raw")
        yaml_file = tmp_path / "protocol.yaml"
        yaml_file.write_text("TEST:\n  processedsamples:\n    - PS: {}")
        script_file = tmp_path / "script.py"
        script_file.write_text("broken")
        output_file = tmp_path / "output.csv"
        
        conversation = DummyConversation()
        # LLM returns code wrapped in markdown
        client = SimpleNamespace(get_response=AsyncMock(return_value="```python\nprint('cleaned')\n```"))
        
        call_count = [0]
        
        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                # First execution fails
                return Mock(returncode=1, stdout="", stderr="Error")
            else:
                # Second execution succeeds
                output_file.write_text(
                    "raw_data_identifier,biosample_id,biosample_name,match_confidence,"
                    "processedsample_placeholder,material_processing_protocol_id\n"
                    "test.raw,,,medium,PS,TEST"
                )
                return Mock(returncode=0, stdout="", stderr="")
        
        with patch('nmdc_dp_utils.llm.biosample_mapping.pipeline.subprocess.run') as mock_run:
            mock_run.side_effect = side_effect
            
            result = asyncio.run(validate_and_fix_script(
                llm_client=client,
                conversation_obj=conversation,
                script_path=str(script_file),
                output_path=str(output_file),
                biosample_path=str(biosample_file),
                files_path=str(files_file),
                yaml_path=str(yaml_file),
                max_iterations=3
            ))
        
        # Check that cleaned code was written to file
        assert result is True
        with open(script_file) as f:
            content = f.read()
            assert "```" not in content
            assert "print('cleaned')" in content


class TestAddStudyDataToConversation:
    """Tests for add_study_data_to_conversation function."""

    def test_adds_minimal_biosample_data(self, tmp_path):
        """Test that only id and name columns are added from biosample CSV."""
        biosample_file = tmp_path / "biosamples.csv"
        biosample_file.write_text(
            "id,name,description,extra_field\n"
            "nmdc:bsm-12-abc123,Sample A,Test desc,extra"
        )
        
        files_file = tmp_path / "files.csv"
        files_file.write_text("file_name\ntest.raw")
        
        yaml_file = tmp_path / "protocol.yaml"
        yaml_file.write_text("TEST:\n  steps: []")
        
        conversation = DummyConversation()
        
        asyncio.run(add_study_data_to_conversation(
            conversation_obj=conversation,
            biosample_attributes_path=str(biosample_file),
            raw_files_path=str(files_file),
            material_processing_yaml_path=str(yaml_file)
        ))
        
        # Find biosample message
        biosample_msg = [m for m in conversation.messages if 'Biosample attributes' in m['content']][0]
        
        # Should contain id and name
        assert 'id,name' in biosample_msg['content']
        assert 'Sample A' in biosample_msg['content']
        # Should NOT contain description or extra_field
        assert 'description' not in biosample_msg['content']
        assert 'extra_field' not in biosample_msg['content']

    def test_adds_minimal_files_data(self, tmp_path):
        """Test that only file_name column is added from files CSV."""
        biosample_file = tmp_path / "biosamples.csv"
        biosample_file.write_text("id,name\ntest,Test")
        
        files_file = tmp_path / "files.csv"
        files_file.write_text("file_name,file_size,checksum\ntest.raw,1000,abc123")
        
        yaml_file = tmp_path / "protocol.yaml"
        yaml_file.write_text("TEST: {}")
        
        conversation = DummyConversation()
        
        asyncio.run(add_study_data_to_conversation(
            conversation_obj=conversation,
            biosample_attributes_path=str(biosample_file),
            raw_files_path=str(files_file),
            material_processing_yaml_path=str(yaml_file)
        ))
        
        files_msg = [m for m in conversation.messages if 'Raw mass spectrometry files' in m['content']][0]
        
        # Should contain file_name
        assert 'file_name' in files_msg['content']
        assert 'test.raw' in files_msg['content']
        # Should NOT contain file_size or checksum
        assert 'file_size' not in files_msg['content']
        assert 'checksum' not in files_msg['content']

    def test_simplifies_yaml_data(self, tmp_path):
        """Test that YAML is simplified to keep only essential fields."""
        biosample_file = tmp_path / "biosamples.csv"
        biosample_file.write_text("id\ntest")
        
        files_file = tmp_path / "files.csv"
        files_file.write_text("file_name\ntest.raw")
        
        yaml_file = tmp_path / "protocol.yaml"
        yaml_file.write_text("""LCMS_POS:
  processedsamples:
    - PS_001: {}
  steps:
    - step1:
        extraction:
          description: "Extract samples"
          has_input:
            - nmdc:bsm-12-abc123
          has_output:
            - PS_001
          start_date: "2024-01-01"
          instruments: ["MS1"]
          extra_field: "should be removed"
""")
        
        conversation = DummyConversation()
        
        asyncio.run(add_study_data_to_conversation(
            conversation_obj=conversation,
            biosample_attributes_path=str(biosample_file),
            raw_files_path=str(files_file),
            material_processing_yaml_path=str(yaml_file)
        ))
        
        yaml_msg = [m for m in conversation.messages if 'Material processing protocol' in m['content']][0]
        
        # Should contain essential fields
        assert 'description' in yaml_msg['content']
        assert 'has_input' in yaml_msg['content']
        assert 'has_output' in yaml_msg['content']
        assert 'processedsamples' in yaml_msg['content']
        # Should NOT contain extra fields
        assert 'start_date' not in yaml_msg['content']
        assert 'instruments' not in yaml_msg['content']
        assert 'extra_field' not in yaml_msg['content']

    def test_adds_additional_context_when_provided(self, tmp_path):
        """Test that additional context file is added if provided."""
        biosample_file = tmp_path / "biosamples.csv"
        biosample_file.write_text("id\ntest")
        files_file = tmp_path / "files.csv"
        files_file.write_text("file_name\ntest.raw")
        yaml_file = tmp_path / "protocol.yaml"
        yaml_file.write_text("TEST: {}")
        context_file = tmp_path / "context.txt"
        context_file.write_text("Sample naming convention: SAMPLE_X_MODE.raw")
        
        conversation = DummyConversation()
        
        asyncio.run(add_study_data_to_conversation(
            conversation_obj=conversation,
            biosample_attributes_path=str(biosample_file),
            raw_files_path=str(files_file),
            material_processing_yaml_path=str(yaml_file),
            additional_context_path=str(context_file)
        ))
        
        # Should have 4 messages (biosample, yaml, files, additional context)
        assert len(conversation.messages) == 4
        
        context_msg = conversation.messages[3]
        assert 'Additional context' in context_msg['content']
        assert 'Sample naming convention' in context_msg['content']

    def test_handles_alternate_file_column_names(self, tmp_path):
        """Test handling of raw_data_file_name column."""
        biosample_file = tmp_path / "biosamples.csv"
        biosample_file.write_text("id\ntest")
        
        files_file = tmp_path / "files.csv"
        files_file.write_text("raw_data_file_name\ntest.raw")
        
        yaml_file = tmp_path / "protocol.yaml"
        yaml_file.write_text("TEST: {}")
        
        conversation = DummyConversation()
        
        asyncio.run(add_study_data_to_conversation(
            conversation_obj=conversation,
            biosample_attributes_path=str(biosample_file),
            raw_files_path=str(files_file),
            material_processing_yaml_path=str(yaml_file)
        ))
        
        files_msg = [m for m in conversation.messages if 'Raw mass spectrometry files' in m['content']][0]
        
        assert 'raw_data_file_name' in files_msg['content']
        assert 'test.raw' in files_msg['content']
