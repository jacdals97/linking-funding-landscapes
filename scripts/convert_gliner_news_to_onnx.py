#!/usr/bin/env python3
import argparse
import os

import torch
from gliner import GLiNER


def export_onnx(model_id: str, output_dir: str, quantize: bool) -> None:
    model = GLiNER.from_pretrained(model_id)
    model.save_pretrained(output_dir)

    gliner_model = GLiNER.from_pretrained(output_dir, load_tokenizer=True)
    gliner_model.model.eval()

    onnx_save_path = os.path.join(output_dir, "model.onnx")

    text = (
        "ONNX is an open-source format designed to enable the interoperability "
        "of AI models across various frameworks and tools."
    )
    labels = ["format", "model", "tool", "cat"]

    inputs, _ = gliner_model.prepare_model_inputs([text], labels)

    if gliner_model.config.span_mode == "token_level":
        all_inputs = (
            inputs["input_ids"],
            inputs["attention_mask"],
            inputs["words_mask"],
            inputs["text_lengths"],
        )
        input_names = ["input_ids", "attention_mask", "words_mask", "text_lengths"]
        dynamic_axes = {
            "input_ids": {0: "batch_size", 1: "sequence_length"},
            "attention_mask": {0: "batch_size", 1: "sequence_length"},
            "words_mask": {0: "batch_size", 1: "sequence_length"},
            "text_lengths": {0: "batch_size", 1: "value"},
            "logits": {
                0: "position",
                1: "batch_size",
                2: "sequence_length",
                3: "num_classes",
            },
        }
    else:
        all_inputs = (
            inputs["input_ids"],
            inputs["attention_mask"],
            inputs["words_mask"],
            inputs["text_lengths"],
            inputs["span_idx"],
            inputs["span_mask"],
        )
        input_names = [
            "input_ids",
            "attention_mask",
            "words_mask",
            "text_lengths",
            "span_idx",
            "span_mask",
        ]
        dynamic_axes = {
            "input_ids": {0: "batch_size", 1: "sequence_length"},
            "attention_mask": {0: "batch_size", 1: "sequence_length"},
            "words_mask": {0: "batch_size", 1: "sequence_length"},
            "text_lengths": {0: "batch_size", 1: "value"},
            "span_idx": {0: "batch_size", 1: "num_spans", 2: "idx"},
            "span_mask": {0: "batch_size", 1: "num_spans"},
            "logits": {
                0: "batch_size",
                1: "sequence_length",
                2: "num_spans",
                3: "num_classes",
            },
        }

    print("Converting the model to ONNX...")
    torch.onnx.export(
        gliner_model.model,
        all_inputs,
        f=onnx_save_path,
        input_names=input_names,
        output_names=["logits"],
        dynamic_axes=dynamic_axes,
        opset_version=14,
    )

    if quantize:
        from onnxruntime.quantization import QuantType, quantize_dynamic

        quantized_save_path = os.path.join(output_dir, "model_quantized.onnx")
        print("Quantizing the ONNX model...")
        quantize_dynamic(
            onnx_save_path,
            quantized_save_path,
            weight_type=QuantType.QUInt8,
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download a GLiNER model and export it to ONNX."
    )
    parser.add_argument(
        "--model-id",
        default="EmergentMethods/gliner_small_news-v2.1",
        help="Hugging Face model ID to download.",
    )
    parser.add_argument(
        "--output-dir",
        default="gliner_news_small",
        help="Directory to save the model artifacts.",
    )
    parser.add_argument(
        "--skip-quantize",
        action="store_true",
        help="Skip creating a quantized ONNX model.",
    )
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    export_onnx(args.model_id, args.output_dir, quantize=not args.skip_quantize)


if __name__ == "__main__":
    main()
