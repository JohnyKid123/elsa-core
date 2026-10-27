using Elsa.Common;
using Elsa.Common.RecurringTasks;
using JetBrains.Annotations;

namespace Elsa.Workflows.Runtime.Tasks;

/// <summary>
/// Re-dispatches cancellation for running instances left in <see cref="WorkflowSubStatus.Cancelling"/>
/// longer than <c>RuntimeOptions.CancellingRecoveryThreshold</c>.
/// </summary>
[SingleNodeTask]
[UsedImplicitly]
public sealed class RecoverCancellingWorkflowsTask(ICancellingRecoveryScanner scanner) : RecurringTask
{
    /// <inheritdoc />
    public override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        await scanner.ScanAndRedispatchAsync(cancellationToken);
    }
}
