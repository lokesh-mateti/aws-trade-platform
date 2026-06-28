resource "aws_cloudwatch_event_bus" "trade_events" {
  name = "${var.project}-bus-${var.environment}"
}

resource "aws_cloudwatch_event_rule" "trade_ingest" {
  name           = "${var.project}-ingest-rule-${var.environment}"
  description    = "Routes trade events from producer to Lambda"
  event_bus_name = aws_cloudwatch_event_bus.trade_events.name

  event_pattern = jsonencode({
    source      = ["com.jpmorgandemo.tradestream"]
    detail-type = ["TradeEvent"]
  })
}

resource "aws_cloudwatch_event_target" "lambda" {
  rule           = aws_cloudwatch_event_rule.trade_ingest.name
  event_bus_name = aws_cloudwatch_event_bus.trade_events.name
  target_id      = "TradeIngestLambda"
  arn            = aws_lambda_function.trade_ingest.arn
}
