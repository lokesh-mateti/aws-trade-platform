output "raw_bucket_name" {
  description = "S3 raw bucket name"
  value       = aws_s3_bucket.raw.bucket
}

output "processed_bucket_name" {
  description = "S3 processed bucket name"
  value       = aws_s3_bucket.processed.bucket
}

output "event_bus_name" {
  description = "EventBridge bus name"
  value       = aws_cloudwatch_event_bus.trade_events.name
}

output "lambda_function_name" {
  description = "Lambda function name"
  value       = aws_lambda_function.trade_ingest.function_name
}
