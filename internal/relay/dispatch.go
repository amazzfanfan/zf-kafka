package relay

import (
	"context"
	"encoding/json"
	"golang.org/x/time/rate"
	"log/slog"
	"sync"
	"time"
)

type endpointBudget struct {
	limiter   *rate.Limiter
	inflight  int
	signature string
	failures  int
	openUntil time.Time
}

// A single elected dispatcher owns global/endpoint quotas. Multiple API/ingest
// processes are allowed; dispatch failover conservatively waits out old leases.
type Dispatcher struct {
	Store       *Store
	Sender      *Sender
	MaxInflight int
}

func (d *Dispatcher) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if e := d.lead(ctx); e != nil && ctx.Err() == nil {
			slog.Warn("dispatcher leadership ended", "reason", e.Error())
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}
func (d *Dispatcher) lead(ctx context.Context) error {
	conn, e := d.Store.DB.Acquire(ctx)
	if e != nil {
		return e
	}
	defer conn.Release()
	var locked bool
	if e = conn.QueryRow(ctx, "SELECT pg_try_advisory_lock(731994)").Scan(&locked); e != nil {
		return e
	}
	if !locked {
		return nil
	}
	defer conn.Exec(context.Background(), "SELECT pg_advisory_unlock(731994)")
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }()
	slog.Info("dispatcher elected")
	var previousUntil time.Time
	if e = conn.QueryRow(ctx, "SELECT COALESCE(max(lease_until),now()) FROM deliveries WHERE status='SENDING'").Scan(&previousUntil); e != nil {
		return e
	}
	wait := max(time.Duration(0), time.Until(previousUntil)+time.Second)
	select {
	case <-child.Done():
		return child.Err()
	case <-time.After(wait):
	}
	// Dedicated connection lock is monitored; losing it stops all new sends.
	var mu sync.Mutex
	budgets := map[string]*endpointBudget{}
	global := 0
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	pingAt := time.Now()
	recoverAt := time.Now()
	cleanupAt := time.Now()
	rotation := 0
	wake := make(chan struct{}, 1)
	var list []json.RawMessage
	var configAt time.Time
	for {
		select {
		case <-child.Done():
			return child.Err()
		case <-tick.C:
		case <-wake:
		}
		{
			if time.Since(cleanupAt) > time.Minute {
				cleanupAt = time.Now()
				if e = d.Store.Cleanup(child); e != nil {
					return e
				}
			}
			if time.Since(pingAt) > time.Second {
				pingAt = time.Now()
				pctx, pcancel := context.WithTimeout(child, time.Second)
				e = conn.Conn().Ping(pctx)
				pcancel()
				if e != nil {
					cancel()
					return e
				}
			}
			if time.Since(recoverAt) > time.Second {
				recoverAt = time.Now()
				if e = d.Store.Recover(child); e != nil {
					return e
				}
			}
			if time.Since(configAt) >= time.Second {
				list, e = d.Store.List(child, "endpoints")
				if e != nil {
					return e
				}
				configAt = time.Now()
			}
			for i := 0; i < len(list); i++ {
				b := list[(i+rotation)%len(list)]
				var ep Endpoint
				if json.Unmarshal(b, &ep) != nil || !ep.Enabled {
					continue
				}
				mu.Lock()
				budget := budgets[ep.ID]
				sig := string(b)
				if budget == nil {
					budget = &endpointBudget{limiter: rate.NewLimiter(rate.Limit(ep.RPS), max(1, int(ep.RPS))), signature: sig}
					budgets[ep.ID] = budget
				} else if budget.signature != sig {
					budget.limiter.SetLimit(rate.Limit(ep.RPS))
					budget.limiter.SetBurst(max(1, int(ep.RPS)))
					budget.signature = sig
				}
				free := min(ep.Concurrency-budget.inflight, d.MaxInflight-global, 64)
				if time.Now().Before(budget.openUntil) {
					free = 0
				}
				if !budget.openUntil.IsZero() && free > 0 {
					if budget.inflight > 0 {
						free = 0
					} else {
						free = 1
					}
				}
				n := min(free, int(budget.limiter.Tokens()))
				mu.Unlock()
				if n <= 0 {
					continue
				}
				tasks, e := d.Store.Claim(child, ep.ID, n)
				if e != nil {
					return e
				}
				for _, task := range tasks {
					task.Endpoint.AllowPrivate = task.Endpoint.AllowPrivate && ep.AllowPrivate
					mu.Lock()
					budget.limiter.Allow()
					budget.inflight++
					global++
					mu.Unlock()
					wg.Add(1)
					go func(task Delivery, budget *endpointBudget) {
						defer wg.Done()
						defer func() {
							mu.Lock()
							budget.inflight--
							global--
							mu.Unlock()
							select {
							case wake <- struct{}{}:
							default:
							}
						}()
						result := d.Sender.Send(child, task)
						mu.Lock()
						if result.Status == "RETRY_WAIT" {
							budget.failures++
							if budget.failures >= 5 {
								budget.openUntil = time.Now().Add(30 * time.Second)
							}
						} else if result.Status == "SUCCEEDED" {
							budget.failures = 0
							budget.openUntil = time.Time{}
						}
						mu.Unlock()
						saveCtx, saveCancel := context.WithTimeout(context.Background(), 3*time.Second)
						defer saveCancel()
						if err := d.Store.Complete(saveCtx, task, result); err != nil {
							slog.Error("cannot persist delivery result", "delivery", task.ID)
						}
					}(task, budget)
				}
			}
			if len(list) > 0 {
				rotation = (rotation + 1) % len(list)
			}
		}
	}
}
