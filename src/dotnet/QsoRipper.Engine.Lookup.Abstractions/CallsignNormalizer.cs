namespace QsoRipper.Engine.Lookup;

/// <summary>Creates exact callsign keys for lookup and cache operations.</summary>
public static class CallsignNormalizer
{
    /// <summary>Trim and uppercase a callsign while preserving slash components.</summary>
    public static string Normalize(string callsign)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(callsign);
        return callsign.Trim().ToUpperInvariant();
    }
}
