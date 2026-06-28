resource "aws_lambda_function" "trade_ingest" {
  function_name = "${var.project}-ingest-${var.environment}"
  description   = "Validates and writes trade events from EventBridge to S3 raw bucket"
  role          = aws_iam_role.lambda_exec.arn

  filename         = "${path.module}/../lambda/trade_ingest.zip"
  source_code_hash = filebase64sha256("${path.module}/../lambda/trade_ingest.zip")
  handler          = "trade_ingest.handler"
  runtime          = "python3.12"
  timeout          = 30
  memory_size      = 128

  environment {
    variables = {
      RAW_BUCKET  = aws_s3_bucket.raw.bucket
      ENVIRONMENT = var.environment
    }
  }
}

resource "aws_lambda_permission" "eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.trade_ingest.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.trade_ingest.arn
}
