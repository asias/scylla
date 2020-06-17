/*
 * Copyright 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <string>
#include <unordered_set>

#include "rjson.hh"
#include "schema_fwd.hh"

#include "expressions_types.hh"

namespace alternator {

// calculate_value() behaves slightly different (especially, different
// functions supported) when used in different types of expressions, as
// enumerated in this enum:
enum class calculate_value_caller {
    UpdateExpression, ConditionExpression, ConditionExpressionAlone
};

inline std::ostream& operator<<(std::ostream& out, calculate_value_caller caller) {
    switch (caller) {
        case calculate_value_caller::UpdateExpression:
            out << "UpdateExpression";
            break;
        case calculate_value_caller::ConditionExpression:
            out << "ConditionExpression";
            break;
        case calculate_value_caller::ConditionExpressionAlone:
            out << "ConditionExpression";
            break;
        default:
            out << "unknown type of expression";
            break;
    }
    return out;
}

bool check_CONTAINS(const rjson::value* v1, const rjson::value& v2);

rjson::value calculate_value(const parsed::value& v,
        calculate_value_caller caller,
        const rjson::value* expression_attribute_values,
        std::unordered_set<std::string>& used_attribute_names,
        std::unordered_set<std::string>& used_attribute_values,
        const rjson::value& update_info,
        schema_ptr schema,
        const std::unique_ptr<rjson::value>& previous_item);

bool verify_condition_expression(
        const parsed::condition_expression& condition_expression,
        std::unordered_set<std::string>& used_attribute_values,
        std::unordered_set<std::string>& used_attribute_names,
        const rjson::value& req,
        schema_ptr schema,
        const std::unique_ptr<rjson::value>& previous_item);

} /* namespace alternator */
