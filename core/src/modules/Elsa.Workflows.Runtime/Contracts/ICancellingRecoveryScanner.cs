namespace Elsa.Workflows.Runtime;

/// <summary>
/// Scans for running workflow instances left in <see cref="WorkflowSubStatus.Cancelling"/> after a lost cancellation enqueue,
/// and re-dispatches cancellation for each row older than <c>RuntimeOptions.CancellingRecoveryThreshold</c>.
/// </summary>
/// <remarks>
/// Runs once at startup and on <c>RecoverCancellingWorkflowsTask</c>. The age check is what leaves an in-flight cancel alone.
/// This is separate from <see cref="IInterruptedRecoveryScanner"/>, which restarts <see cref="WorkflowSubStatus.Interrupted"/> rows.
/// </remarks>
public interface ICancellingRecoveryScanner
{
    /// <summary>
    /// Claims each stale Cancelling instance and re-dispatches cancellation. Returns the number of dispatches issued.
    /// </summary>
    ValueTask<int> ScanAndRedispatchAsync(CancellationToken cancellationToken);
}
