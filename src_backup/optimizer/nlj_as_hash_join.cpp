//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nlj_as_hash_join.cpp
//
// Identification: src/optimizer/nlj_as_hash_join.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

#include "execution/expressions/logic_expression.h"

#include <algorithm>
#include <memory>
namespace bustub {

/**
 * @brief optimize nested loop join into hash join.
 * In the starter code, we will check NLJs with exactly one equal condition. You
 * can further support optimizing joins with multiple eq conditions.
 */
auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan)
    -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for Spring 2025: You should support join keys of any number of
  // conjunction of equi-conditions: E.g. <column expr> = <column expr> AND
  // <column expr> = <column expr> AND ...
  // 1. 递归优化所有的子节点 (自底向上，先把底下的图纸改好)
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  // 2. 如果当前图纸不是 NestedLoopJoin，就不关我们 HashJoin 的事，原样退回
  if (optimized_plan->GetType() != PlanType::NestedLoopJoin) {
    return optimized_plan;
  }

  // 3. 强转成 NestedLoopJoin 图纸，准备开刀
  const auto &nlj_plan =
      dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
  auto predicate = nlj_plan.Predicate();

  // 如果根本没有条件 (比如 CROSS JOIN 笛卡尔积)，那绝不能用 Hash Join
  if (predicate == nullptr) {
    return optimized_plan;
  }

  std::vector<AbstractExpressionRef> left_key_exprs;
  std::vector<AbstractExpressionRef> right_key_exprs;

  // 4. 定义一个递归小助手：专门用来拆解 A = B AND C = D 这种条件
  std::function<bool(const AbstractExpressionRef &)> extract_equi_keys =
      [&](const AbstractExpressionRef &expr) -> bool {
    // 遇到了 AND 逻辑符，那就左右两边都要继续拆！
    if (auto logic_expr = dynamic_cast<const LogicExpression *>(expr.get());
        logic_expr != nullptr) {
      if (logic_expr->logic_type_ == LogicType::And) {
        return extract_equi_keys(logic_expr->GetChildAt(0)) &&
               extract_equi_keys(logic_expr->GetChildAt(1));
      }
      return false; // 如果是 OR，Hash Join 搞不定，放弃治疗
    }

    // 遇到了比较符，必须是等于号 (=)
    if (auto cmp_expr = dynamic_cast<const ComparisonExpression *>(expr.get());
        cmp_expr != nullptr) {
      if (cmp_expr->comp_type_ == ComparisonType::Equal) {
        auto left_child = cmp_expr->GetChildAt(0);
        auto right_child = cmp_expr->GetChildAt(1);

        // 强转成列名表达式 (比如 t1.id)，join一定是两个表相互连接，不会有常量
        auto left_col =
            dynamic_cast<const ColumnValueExpression *>(left_child.get());
        auto right_col =
            dynamic_cast<const ColumnValueExpression *>(right_child.get());

        if (left_col != nullptr && right_col != nullptr) {
          // 判断谁是左表 (TupleIdx == 0)，谁是右表 (TupleIdx == 1)
          if (left_col->GetTupleIdx() == 0 && right_col->GetTupleIdx() == 1) {
            left_key_exprs.push_back(left_child);
            right_key_exprs.push_back(right_child);
            return true;
          }
          // SQL反了(比如 t2.id = t1.id)
          if (left_col->GetTupleIdx() == 1 && right_col->GetTupleIdx() == 0) {
            left_key_exprs.push_back(right_child);
            right_key_exprs.push_back(left_child);
            return true;
          }
        }
      }
    }
    // 如果不是 AND，也不是等于号，或者两边不是列名，统统拒绝！
    return false;
  };

  // 如果条件全部符合“纯等值连接”的要求
  if (extract_equi_keys(predicate)) {
    // 换上我们的 HashJoin 机器图纸
    return std::make_shared<HashJoinPlanNode>(
        nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
        nlj_plan.GetRightPlan(), left_key_exprs, right_key_exprs,
        nlj_plan.GetJoinType());
  }

  return optimized_plan;
}

} // namespace bustub
