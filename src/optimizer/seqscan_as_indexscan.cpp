//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seqscan_as_indexscan.cpp
//
// Identification: src/optimizer/seqscan_as_indexscan.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/optimizer.h"

#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
namespace bustub {

/**
 * @brief Optimizes seq scan as index scan if there's an index on a table
 */

// 辅助函数：深度优先遍历表达式树，挖出哪怕藏在 OR 里的列下标！
// 有的时候可能是 xx or xx 这种相对负责的情况，我们就需要找一遍有没有 v = 2这种
auto GetColIdx(const AbstractExpressionRef &expr) -> std::optional<uint32_t> {
  if (auto col_expr = dynamic_cast<const ColumnValueExpression *>(expr.get())) {
    return col_expr->GetColIdx();
  }
  for (const auto &child : expr->GetChildren()) {
    if (auto res = GetColIdx(child)) {
      return res;  // 只要找到一个列，就赶紧汇报
    }
  }
  return std::nullopt;
}

// 辅助函数：只对最简单的 "单点等值查询" 提取常量狙击子弹
// xxx or xxx 会被认为复杂类型，所以无法通过单点等值查询
auto GetConstantFromEqual(const AbstractExpressionRef &expr) -> std::optional<AbstractExpressionRef> {
  auto cmp_expr = dynamic_cast<const ComparisonExpression *>(expr.get());
  if (cmp_expr != nullptr && cmp_expr->comp_type_ == ComparisonType::Equal) {
    auto left_col = dynamic_cast<const ColumnValueExpression *>(cmp_expr->GetChildAt(0).get());
    auto right_const = dynamic_cast<const ConstantValueExpression *>(cmp_expr->GetChildAt(1).get());
    if (left_col != nullptr && right_const != nullptr) {
      return cmp_expr->GetChildAt(1);
    }

    auto left_const = dynamic_cast<const ConstantValueExpression *>(cmp_expr->GetChildAt(0).get());
    auto right_col = dynamic_cast<const ColumnValueExpression *>(cmp_expr->GetChildAt(1).get());
    if (left_const != nullptr && right_col != nullptr) {
      return cmp_expr->GetChildAt(0);
    }
  }
  return std::nullopt;
}
auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(P3): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp
  // when forcing starter rule 谓词下推
  std::vector<bustub::AbstractPlanNodeRef> children;
  for (auto const &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  // 开始判断能不能转换了
  // 判断类型
  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    // 判断是否存在where子句
    if (seq_scan.filter_predicate_ != nullptr) {
      // !如果遇到了 xx or xx，会被漏掉
      //   // 判断是不是 == 等值比较
      //   // 声明一个比较表达式
      //   auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(
      //       seq_scan.filter_predicate_.get());
      //   if (cmp_expr != nullptr &&
      //       cmp_expr->comp_type_ == ComparisonType::Equal) {
      //     // 一个是列，一个是常量
      //     // 要考虑 v=2 ， 2=v
      //     const auto *left_column = dynamic_cast<const ColumnValueExpression
      //     *>(cmp_expr->GetChildAt(0).get()); const auto *right_constant
      //     =dynamic_cast<const ConstantValueExpression
      //     *>(cmp_expr->GetChildAt(1).get());

      //     const auto *left_constant =
      //         dynamic_cast<const ConstantValueExpression *>(
      //             cmp_expr->GetChildAt(0).get());
      //     const auto *right_column = dynamic_cast<const ColumnValueExpression
      //     *>(
      //         cmp_expr->GetChildAt(1).get());

      //     uint32_t col_index;
      //     AbstractExpressionRef constant_expr;

      //     // 拿到这列是表中的第几列
      //     // col_index = left_expr->GetColIdx();
      //     // 去b+tree中翻找
      //     if (left_column != nullptr && right_constant != nullptr) {
      //       col_index = left_column->GetColIdx();
      //       constant_expr = cmp_expr->GetChildAt(1);
      //     } else if (right_column != nullptr && left_constant != nullptr) {
      //       col_index = right_column->GetColIdx();
      //       constant_expr = cmp_expr->GetChildAt(0);
      //     }
      //     else {
      //       return optimized_plan;
      //     }
      //     auto table_info_ = catalog_.GetTable(seq_scan.GetTableOid());
      //     auto table_indexes_ = catalog_.GetTableIndexes(table_info_->name_);
      //     for (const auto &index : table_indexes_) {
      //       const auto &attrs = index->index_->GetKeyAttrs();
      //       // 确保是一个单列索引 , 并且和表中数据是对应
      //       if (attrs.size() == 1 && attrs[0] == col_index) {
      //         // 用常量去做indexscan的参数
      //         std::vector<AbstractExpressionRef> pred_keys_{constant_expr};
      //         return std::make_shared<IndexScanPlanNode>(
      //             seq_scan.output_schema_, seq_scan.table_oid_,
      //             index->index_oid_, seq_scan.filter_predicate_, pred_keys_);
      //       }
      //     }
      //   }
      auto col_idx_opt = GetColIdx(seq_scan.filter_predicate_);
      if (col_idx_opt.has_value()) {
        // 第几列
        uint32_t col_index = col_idx_opt.value();
        auto table_info = catalog_.GetTable(seq_scan.GetTableOid());
        auto table_indexes = catalog_.GetTableIndexes(table_info->name_);

        for (const auto &index : table_indexes) {
          const auto &attrs = index->index_->GetKeyAttrs();
          // 保证是单列，并且是表中的这一列
          if (attrs.size() == 1 && attrs[0] == col_index) {
            std::vector<AbstractExpressionRef> pred_keys;
            auto constant_opt = GetConstantFromEqual(seq_scan.filter_predicate_);

            if (constant_opt.has_value()) {
              pred_keys.push_back(constant_opt.value());
            }

            return std::make_shared<IndexScanPlanNode>(seq_scan.output_schema_, seq_scan.table_oid_, index->index_oid_,
                                                       seq_scan.filter_predicate_, pred_keys);
          }
        }
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
