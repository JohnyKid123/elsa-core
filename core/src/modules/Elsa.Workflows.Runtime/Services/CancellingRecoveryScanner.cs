using Elsa.Common;
using Elsa.Common.Multitenancy;
using Elsa.Workflows.Management;
using Elsa.Workflows.Management.Filters;
using Elsa.Workflows.Management.Models;
using Elsa.Workflows.Runtime.Options;
using Elsa.Workflows.Runtime.Requests;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Elsa.Workflows.Runtime.Services;

/// <summary>
/// Default implementation of <see cref="ICancellingRecoveryScanner"/>.
/// </summary>
public sealed class CancellingRecoveryScanner(
    IWorkflowCancellationDispatcher dispatcher,
    IWorkflowInstanceStore instanceStore,
    IOptions<RuntimeOptions> runtimeOptions,
    ISystemClock systemClock,
    ILogger<CancellingRecoveryScanner> logger,
    ITenantService? tenantService = null,
    ITenantAccessor? tenantAccessor = null) : ICancellingRecoveryScanner
{
    /// <inheritdoc />
    public async ValueTask<int> ScanAndRedispatchAsync(CancellationToken cancellationToken)
    {
        var options = runtimeOptions.Value;
        var threshold = options.CancellingRecoveryThreshold;
        if (threshold <= TimeSpan.Zero)
        {
            logger.LogDebug("Cancelling recovery is disabled because CancellingRecoveryThreshold is not positive.");
            return 0;
        }

        var batchSize = Math.Max(1, options.CancellingRecoveryBatchSize);
        var cutoff = systemClock.UtcNow - threshold;
        var filter = new WorkflowInstanceFilter
        {
            WorkflowStatus = WorkflowStatus.Running,
            WorkflowSubStatus = WorkflowSubStatus.Cancelling,
            IsExecuting = false,
            BeforeLastUpdated = cutoff
        };

        // The claim refreshes UpdatedAt, which drops the row out of this filter.
        // Read the matches first so later pages are not skipped.
        var stale = new List<WorkflowInstanceSummary>();
        await foreach (var summary in instanceStore.EnumerateSummariesAsync(filter, batchSize, cancellationToken))
            stale.Add(summary);

        logger.LogInformation("Scanning {Count} Cancelling workflow(s) older than {Threshold}.", stale.Count, threshold);

        var redispatched = 0;
        foreach (var summary in stale)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                if (await TryRedispatchAsync(summary, cancellationToken))
                    redispatched++;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (!ex.IsFatal())
            {
                logger.LogError(
                    ex,
                    "Failed to re-dispatch cancellation for workflow {WorkflowInstanceId}. The next scan retries it after the recovery threshold.",
                    summary.Id);
            }
        }

        logger.LogInformation("Cancelling recovery scan complete; re-dispatched {Count} instance(s).", redispatched);
        return redispatched;
    }

    private async ValueTask<bool> TryRedispatchAsync(WorkflowInstanceSummary summary, CancellationToken cancellationToken)
    {
        if (summary.UpdatedAt is not { } cancellingAt)
        {
            logger.LogDebug("Workflow instance {WorkflowInstanceId} is Cancelling without UpdatedAt. Skipping recovery.", summary.Id);
            return false;
        }

        var tenantId = summary.TenantId ?? string.Empty;
        if (tenantService is not null && tenantAccessor is not null && !string.IsNullOrWhiteSpace(tenantId) && tenantId != Tenant.AgnosticTenantId)
        {
            var tenant = await tenantService.FindAsync(tenantId, cancellationToken) ?? new Tenant { Id = tenantId, Name = tenantId };
            using (tenantAccessor.PushContext(tenant))
                return await ClaimAndDispatchAsync(summary.Id, cancellingAt, cancellationToken);
        }

        return await ClaimAndDispatchAsync(summary.Id, cancellingAt, cancellationToken);
    }

    private async ValueTask<bool> ClaimAndDispatchAsync(string workflowInstanceId, DateTimeOffset cancellingAt, CancellationToken cancellationToken)
    {
        var claimedAt = systemClock.UtcNow.TruncateToMilliseconds();
        var claimed = await instanceStore.TryTransitionSubStatusAsync(
            workflowInstanceId,
            WorkflowSubStatus.Cancelling,
            cancellingAt,
            WorkflowSubStatus.Cancelling,
            claimedAt,
            cancellationToken);

        if (!claimed)
        {
            logger.LogDebug(
                "Workflow instance {WorkflowInstanceId} was not claimed for Cancelling recovery. A concurrent cancel or restore won.",
                workflowInstanceId);
            return false;
        }

        // Omit PreviousSubStatus and CancellingAt. An observed failure of this dispatch must leave Cancelling in place
        // so a later scan can retry. The new stamp also makes an older restore compare-and-swap miss this row.
        await dispatcher.DispatchAsync(new DispatchCancelWorkflowRequest
        {
            WorkflowInstanceId = workflowInstanceId
        }, cancellationToken);

        return true;
    }
}
