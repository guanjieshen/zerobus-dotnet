using Xunit;

namespace Databricks.Solutions.Zerobus.Tests;

public class OffsetTrackerTests
{
    [Fact]
    public void AssignNext_is_monotonic_from_zero()
    {
        var tracker = new OffsetTracker(callback: null);
        Assert.Equal(0, tracker.AssignNext());
        Assert.Equal(1, tracker.AssignNext());
        Assert.Equal(2, tracker.AssignNext());
        Assert.Equal(2, tracker.LastAssigned);
    }

    [Fact]
    public async Task Cumulative_ack_releases_all_waiters_at_or_below_offset()
    {
        var tracker = new OffsetTracker(callback: null);
        foreach (var _ in Enumerable.Range(0, 11)) tracker.AssignNext(); // 0..10

        var w1 = tracker.WaitForOffsetAsync(1, default);
        var w5 = tracker.WaitForOffsetAsync(5, default);
        var w10 = tracker.WaitForOffsetAsync(10, default);

        tracker.ReleaseUpTo(7);

        await w1.WaitAsync(TimeSpan.FromSeconds(1));
        await w5.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.True(w1.IsCompletedSuccessfully);
        Assert.True(w5.IsCompletedSuccessfully);
        Assert.False(w10.IsCompleted);

        tracker.ReleaseUpTo(10);
        await w10.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.True(w10.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task Waiting_for_an_already_acked_offset_completes_immediately()
    {
        var tracker = new OffsetTracker(callback: null);
        tracker.AssignNext();
        tracker.ReleaseUpTo(0);
        await tracker.WaitForOffsetAsync(0, default).WaitAsync(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public void Duplicate_and_out_of_order_acks_do_not_regress()
    {
        var tracker = new OffsetTracker(callback: null);
        foreach (var _ in Enumerable.Range(0, 6)) tracker.AssignNext();
        tracker.ReleaseUpTo(5);
        tracker.ReleaseUpTo(3); // stale, ignored
        tracker.ReleaseUpTo(5); // duplicate, no effect
        Assert.Equal(5, tracker.LastAcked);
    }

    [Fact]
    public async Task Fault_faults_pending_waiters()
    {
        var tracker = new OffsetTracker(callback: null);
        tracker.AssignNext();
        var waiter = tracker.WaitForOffsetAsync(0, default);

        tracker.Fault(new ZerobusStreamClosedException("boom"));

        await Assert.ThrowsAsync<ZerobusStreamClosedException>(() => waiter);
    }

    [Fact]
    public void Callback_fires_once_per_offset_in_order()
    {
        var acked = new List<long>();
        var callback = new RecordingCallback(acked);
        var tracker = new OffsetTracker(callback);
        foreach (var _ in Enumerable.Range(0, 5)) tracker.AssignNext(); // 0..4

        tracker.ReleaseUpTo(2);
        tracker.ReleaseUpTo(4);

        Assert.Equal(new long[] { 0, 1, 2, 3, 4 }, acked);
    }

    private sealed class RecordingCallback : IAckCallback
    {
        private readonly List<long> _acked;
        public RecordingCallback(List<long> acked) => _acked = acked;
        public void OnAck(long offset) => _acked.Add(offset);
        public void OnError(long offset, Exception error) { }
    }
}
