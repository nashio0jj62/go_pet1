package telemetry

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (w *responseWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	if status >= 100 && status < 200 {
		w.ResponseWriter.WriteHeader(status)
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *responseWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(data)
}

func (w *responseWriter) StatusCode() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

func (w *responseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func HTTP(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		r = r.WithContext(ctx)
		started := time.Now()
		writer := &responseWriter{ResponseWriter: w}
		handler.ServeHTTP(writer, r)
		if r.URL.Path == "/healthz" {
			return
		}
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		method := r.Method
		switch method {
		case "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS":
		default:
			method = "other"
		}
		labels := metric.WithAttributes(attribute.String("method", method),
			attribute.String("route", route), attribute.String("status_code", strconv.Itoa(writer.StatusCode())))
		httpRequests.Add(ctx, 1, labels)
		httpDuration.Record(ctx, time.Since(started).Seconds(), labels)
		if writer.StatusCode() >= 400 {
			kind := "http_4xx"
			if writer.StatusCode() >= 500 {
				kind = "http_5xx"
			}
			errorsTotal.Add(ctx, 1, metric.WithAttributes(
				attribute.String("component", "http"), attribute.String("type", kind)))
		}
	})
}

func StartHTTP(ctx context.Context, w http.ResponseWriter, name, paymentID string) (context.Context, func()) {
	ctx, span := Tracer.Start(ctx, name, trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(attribute.String("payment.id", paymentID)))
	if span.SpanContext().IsValid() {
		w.Header().Set("X-Trace-ID", span.SpanContext().TraceID().String())
	}
	return ctx, func() {
		if writer, ok := w.(interface{ StatusCode() int }); ok {
			status := writer.StatusCode()
			span.SetAttributes(attribute.Int("http.response.status_code", status))
			if status >= 400 {
				span.SetStatus(codes.Error, http.StatusText(status))
			}
		}
		span.End()
	}
}

func TraceParent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

func Parent(ctx context.Context, parent string) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier{"traceparent": parent})
}

func TraceID(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ""
	}
	return sc.TraceID().String()
}
