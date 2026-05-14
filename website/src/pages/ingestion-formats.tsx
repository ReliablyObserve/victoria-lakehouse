import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';

const formats = [
  {
    name: 'Loki Push API',
    endpoint: '/insert/loki/api/v1/push',
    signal: 'Logs',
    description: 'Drop-in Loki-compatible endpoint. Point Grafana Alloy, Promtail, Fluentd, or any Loki client directly at Lakehouse. Supports labels, structured metadata, and tenant headers.',
    agents: 'Grafana Alloy, Promtail, Fluentd (loki output), Logstash (loki output)',
  },
  {
    name: 'Elasticsearch Bulk API',
    endpoint: '/_bulk, /{index}/_bulk',
    signal: 'Logs',
    description: 'Full Elasticsearch bulk indexing compatibility. Migrate from ELK/OpenSearch without changing your pipeline. Supports _index routing, _id dedup, and bulk error responses.',
    agents: 'Filebeat, Logstash, Fluentd (elasticsearch output), Vector',
  },
  {
    name: 'OpenTelemetry (OTLP)',
    endpoint: 'gRPC :4317, HTTP :4318',
    signal: 'Logs + Traces',
    description: 'Native OpenTelemetry Protocol support for both logs and traces. Resource attributes, scope attributes, and span events are preserved in Parquet MAP columns. OTEL Collector exports directly.',
    agents: 'OTEL Collector, Grafana Alloy, instrumented applications',
  },
  {
    name: 'Syslog',
    endpoint: 'RFC 3164 / RFC 5424',
    signal: 'Logs',
    description: 'Native syslog protocol support. Forward from rsyslog, syslog-ng, or any RFC-compliant source. Facility, severity, and structured data parsed automatically.',
    agents: 'rsyslog, syslog-ng, systemd-journal-remote',
  },
  {
    name: 'Fluentd Forward',
    endpoint: 'Fluent Forward Protocol',
    signal: 'Logs',
    description: 'Native Fluentd/Fluent Bit forward protocol. Zero-config migration from existing Fluentd pipelines. Supports MessagePack encoding and tag-based routing.',
    agents: 'Fluentd, Fluent Bit',
  },
  {
    name: 'Logstash',
    endpoint: 'HTTP output plugin',
    signal: 'Logs',
    description: 'Logstash HTTP output compatibility. Migrate from ELK stack by changing the output plugin URL. All Logstash filter processing is preserved.',
    agents: 'Logstash',
  },
  {
    name: 'Datadog',
    endpoint: 'Datadog Logs API',
    signal: 'Logs',
    description: 'Datadog-compatible log ingestion API. Redirect Datadog agents to Lakehouse for cost-effective long-term storage while keeping Datadog for real-time alerting.',
    agents: 'Datadog Agent, Vector (datadog_logs sink)',
  },
  {
    name: 'Journald',
    endpoint: 'systemd journal',
    signal: 'Logs',
    description: 'Native systemd journal format support. Collect kernel, service, and audit logs directly from Linux hosts without format conversion.',
    agents: 'vmagent, systemd-journal-upload',
  },
  {
    name: 'NDJSON (JSON Lines)',
    endpoint: '/insert/jsonline',
    signal: 'Logs',
    description: 'Simple newline-delimited JSON ingestion. Send structured logs with any HTTP client. Supports _time, _msg, and _stream_fields conventions.',
    agents: 'curl, any HTTP client, custom applications',
  },
  {
    name: 'Zipkin',
    endpoint: '/api/v2/spans',
    signal: 'Traces',
    description: 'Zipkin v2 span ingestion. Migrate from Zipkin backends without changing instrumentation. Span annotations and tags mapped to Parquet attributes.',
    agents: 'Zipkin-instrumented applications, OTEL Collector (zipkin exporter)',
  },
  {
    name: 'Jaeger',
    endpoint: 'Thrift / gRPC',
    signal: 'Traces',
    description: 'Jaeger Thrift and gRPC protocol support. Migrate from Jaeger backends while keeping the Jaeger UI for trace visualization.',
    agents: 'Jaeger-instrumented applications, OTEL Collector (jaeger exporter)',
  },
];

export default function IngestionFormats() {
  return (
    <Layout
      title="Ingestion Formats — 11+ Log and Trace Ingestion APIs"
      description="Victoria Lakehouse supports 11+ ingestion formats: Loki, Elasticsearch, OTLP, Syslog, Fluentd, Logstash, Datadog, Journald, NDJSON, Zipkin, and Jaeger. Drop-in replacement for existing pipelines. Zero code changes required.">
      <header className="hero hero--primary" style={{padding: '3rem 0', textAlign: 'center'}}>
        <div className="container">
          <h1 className="hero__title">11+ Ingestion Formats</h1>
          <p className="hero__subtitle" style={{maxWidth: 700, margin: '0 auto'}}>
            Every format VictoriaLogs and VictoriaTraces support.
            Point your existing agents at Lakehouse &mdash; zero code changes.
          </p>
        </div>
      </header>

      <main className="container margin-vert--xl">
        <div className="row margin-bottom--xl">
          <div className="col col--8 col--offset-2">
            <div className="admonition admonition-tip alert alert--success">
              <div className="admonition-heading"><h5>Drop-In Replacement</h5></div>
              <div className="admonition-content">
                <p>Victoria Lakehouse implements the <strong>exact same insert APIs</strong> as
                VictoriaLogs and VictoriaTraces. If your agent supports VL/VT, it supports
                Lakehouse. Change the endpoint URL and you're done.</p>
              </div>
            </div>
          </div>
        </div>

        {formats.map((f, i) => (
          <div key={i} className="row margin-bottom--lg">
            <div className="col col--8 col--offset-2">
              <div style={{
                borderLeft: `4px solid ${f.signal === 'Traces' ? '#2563eb' : f.signal === 'Logs + Traces' ? '#7c3aed' : 'var(--ifm-color-primary)'}`,
                padding: '1.5rem',
                background: 'var(--ifm-color-emphasis-100)',
                borderRadius: '0 8px 8px 0',
                marginBottom: '0.5rem',
              }}>
                <div style={{display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '0.5rem'}}>
                  <h3 style={{margin: 0}}>{f.name}</h3>
                  <span style={{
                    fontSize: '0.75rem',
                    textTransform: 'uppercase',
                    letterSpacing: '0.05em',
                    fontWeight: 600,
                    padding: '0.2rem 0.6rem',
                    borderRadius: '4px',
                    background: f.signal === 'Traces' ? 'rgba(59,130,246,0.15)' : f.signal === 'Logs + Traces' ? 'rgba(139,92,246,0.15)' : 'rgba(45,106,79,0.15)',
                    color: f.signal === 'Traces' ? '#2563eb' : f.signal === 'Logs + Traces' ? '#7c3aed' : 'var(--ifm-color-primary-dark)',
                  }}>{f.signal}</span>
                </div>
                <code style={{fontSize: '0.85rem', display: 'block', margin: '0.5rem 0'}}>{f.endpoint}</code>
                <p style={{margin: '0.5rem 0', lineHeight: 1.6}}>{f.description}</p>
                <p style={{margin: 0, fontSize: '0.85rem', color: 'var(--ifm-color-emphasis-600)'}}>
                  <strong>Compatible agents:</strong> {f.agents}
                </p>
              </div>
            </div>
          </div>
        ))}

        <div className="text--center margin-vert--xl">
          <Link to="/docs/getting-started/" className="button button--primary button--lg">
            Quick Start Guide
          </Link>
          {' '}
          <Link to="/docs/write-path/" className="button button--secondary button--lg">
            Write Path Architecture
          </Link>
        </div>
      </main>
    </Layout>
  );
}
