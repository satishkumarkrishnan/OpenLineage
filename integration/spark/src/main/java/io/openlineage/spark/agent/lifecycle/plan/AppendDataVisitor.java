package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PlanUtils;
import java.util.List;
import org.apache.spark.sql.catalyst.plans.logical.AppendData;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;

/**
 * {@link LogicalPlan} visitor that matches an {@link AppendData} commands and extracts the output
 * {@link OpenLineage.Dataset} being written.
 */
public class AppendDataVisitor extends QueryPlanVisitor<AppendData> {
  private final List<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> outputVisitors;

  public AppendDataVisitor(
      List<PartialFunction<LogicalPlan, List<OpenLineage.Dataset>>> outputVisitors) {
    this.outputVisitors = outputVisitors;
  }

  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan x) {
    return PlanUtils.applyFirst(outputVisitors, (LogicalPlan) ((AppendData) x).table());
  }
}