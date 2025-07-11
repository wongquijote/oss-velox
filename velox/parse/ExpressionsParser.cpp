/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "velox/parse/ExpressionsParser.h"
#include "velox/duckdb/conversion/DuckParser.h"

namespace facebook::velox::parse {

core::ExprPtr parseExpr(const std::string& expr, const ParseOptions& options) {
  facebook::velox::duckdb::ParseOptions duckConversionOptions;
  duckConversionOptions.parseDecimalAsDouble = options.parseDecimalAsDouble;
  duckConversionOptions.parseIntegerAsBigint = options.parseIntegerAsBigint;
  duckConversionOptions.functionPrefix = options.functionPrefix;
  return facebook::velox::duckdb::parseExpr(expr, duckConversionOptions);
}

std::vector<core::ExprPtr> parseMultipleExpressions(
    const std::string& expr,
    const ParseOptions& options) {
  facebook::velox::duckdb::ParseOptions duckConversionOptions;
  duckConversionOptions.parseDecimalAsDouble = options.parseDecimalAsDouble;
  duckConversionOptions.parseIntegerAsBigint = options.parseIntegerAsBigint;
  duckConversionOptions.functionPrefix = options.functionPrefix;
  return facebook::velox::duckdb::parseMultipleExpressions(
      expr, duckConversionOptions);
}

OrderByClause parseOrderByExpr(const std::string& expr) {
  auto orderBy = facebook::velox::duckdb::parseOrderByExpr(expr);

  return {
      .expr = std::move(orderBy.expr),
      .ascending = orderBy.ascending,
      .nullsFirst = orderBy.nullsFirst};
}

} // namespace facebook::velox::parse
