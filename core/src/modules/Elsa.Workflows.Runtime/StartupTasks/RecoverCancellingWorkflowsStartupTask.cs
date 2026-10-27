using Elsa.Common;
using Elsa.Common.RecurringTasks;
using JetBrains.Annotations;

namespace Elsa.Workflows.Runtime.StartupTasks;

/// <summary>
/// Startup pass of <see cref="ICancellingRecoveryScanner"/>. A restarted host does not wait for the recurring interval
/// to re-dispatch Cancelling rows that are already older than the recovery threshold.
/// </summary>
[UsedImplicitly]
[SingleNodeTask]
public sealed class RecoverCancellingWorkflowsStartupTask(ICancellingRecoveryScanner scanner) : IStartupTask
{
    /// <inheritdoc />
    public async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        await scanner.ScanAndRedispatchAsync(cancellationToken);
    }
}
