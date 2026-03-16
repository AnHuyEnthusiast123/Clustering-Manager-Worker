// PSO-ACO-GA Version with Improvements
// Mode: -input_mode=continuous (random loop)

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

var (
	mode            = flag.String("mode", "manager", "Run mode: manager or worker")
	workerID        = flag.String("id", "", "Worker ID (A, B, C, D, E) for worker mode")
	inputMode       = flag.String("input_mode", "continuous", "Input mode: continuous")
	configFile      = "task_config.json" // Pre-calculated config
	affinities      = make(map[string]map[string]float64)
	taskConfig      map[string]map[string]map[string]map[string]map[string]float64 // Nested lookup
	ctx             = context.Background()
	rdb             *redis.Client
	conn            *amqp.Connection
	ch              *amqp.Channel
	workers                 = []string{"A", "B", "C", "D", "E"}
	pheromone               = make([]float64, len(workers))
	alpha           float64 = 1.0
	beta            float64 = 1.0
	rho             float64 = 0.2
	affCache                = cache.New(5*time.Second, 10*time.Second)
	statusCache             = cache.New(5*time.Second, 10*time.Second)
	tasksAssigned           = promauto.NewCounterVec(prometheus.CounterOpts{Name: "tasks_assigned_total"}, []string{"worker"})
	taskProcessTime         = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "task_process_time_seconds", Buckets: []float64{5, 10, 15, 20, 25}}, []string{"worker"})
	workerLoad              = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "worker_load"}, []string{"worker"})
	taskErrors              = promauto.NewCounterVec(prometheus.CounterOpts{Name: "task_errors_total"}, []string{"worker"})
	queueLength             = promauto.NewGauge(prometheus.GaugeOpts{Name: "queue_length"})
	totalPower              = promauto.NewCounterVec(prometheus.CounterOpts{Name: "total_power_consumed"}, []string{"worker"})
	taskTypes               = []string{"face_detection", "face_recognition", "heatmap_generate", "transcode_low", "transcode_high"}
	durations               = []float64{30, 60, 90}
	resolutions             = []string{"720p", "1080p"}
	fpsOptions              = []int{20, 30}
	// Hardware simulation: speed <1 = faster, >1 = slower
	workerSpeed = map[string]float64{
		"A": 1.0, // GPU: fast for complex/high-res
		"B": 1.0, // NPU: slower for complex, efficient for lightweight
		"C": 1.0,
		"D": 1.0, // TPU edge: balanced, good for batch
		"E": 1.0,
	}
	// Power factor (relative to baseline): GPU high, NPU low, TPU medium
	workerPowerFactor = map[string]float64{
		"A": 1.0,
		"B": 1.0,
		"C": 1.0,
		"D": 1.0,
		"E": 1.0,
	}
	assignedCounts = make(map[string]float64) // For anti-bias
	taskErrorsMap  = make(map[string]float64)
	totalPowerMap  = make(map[string]float64)
	workerLoadMap  = make(map[string]float64)
	// New: For power in affinity
	avgPowerPerType = make(map[string]float64)
	countPerType    = make(map[string]int)
)

var rabbitUser, rabbitPass string // To use in failOnError

// Group 1: Initialization Functions
func init() {
	flag.Parse()
	seedStr := os.Getenv("RAND_SEED")
	var seed int64
	if seedStr != "" {
		seed, _ = strconv.ParseInt(seedStr, 10, 64)
	} else {
		seed = time.Now().UnixNano()
	}
	rand.Seed(seed)

	redisPass := os.Getenv("REDIS_PASS")
	rdb = redis.NewClient(&redis.Options{Addr: "redis:6379", Password: redisPass, DB: 0})

	rabbitUser := os.Getenv("RABBIT_USER")
	if rabbitUser == "" {
		rabbitUser = "guest"
	}
	rabbitPass := os.Getenv("RABBIT_PASS")
	if rabbitPass == "" {
		rabbitPass = "guest"
	}

	var err error
	conn, err = amqp.Dial(fmt.Sprintf("amqp://%s:%s@rabbitmq:5672/", rabbitUser, rabbitPass))
	failOnError(err, "Failed to connect to RabbitMQ", true)
	ch, err = conn.Channel()
	failOnError(err, "Failed to open a channel", true)
	err = ch.ExchangeDeclare("ai_tasks", "direct", true, false, false, false, nil)
	failOnError(err, "Failed to declare exchange", true)

	for _, w := range workers {
		workerLoad.WithLabelValues(w).Set(0.0)
		workerLoadMap[w] = 0.0
		totalPower.WithLabelValues(w).Add(0.0)
		totalPowerMap[w] = 0.0
		assignedCounts[w] = 0 // Init counts
		taskErrorsMap[w] = 0.0
	}

	loadTaskConfig() // New: Load pre-calculated config

	if *mode == "manager" {
		if _, err := os.Stat("affinities.json"); os.IsNotExist(err) {
			defaultAff := make(map[string]map[string]float64)
			for _, w := range workers {
				defaultAff[w] = make(map[string]float64)
				for _, tt := range taskTypes {
					defaultAff[w][tt] = 0.5
				}
			}
			data, _ := json.MarshalIndent(defaultAff, "", " ")
			os.WriteFile("affinities.json", data, 0644)
		}
		data, err := os.ReadFile("affinities.json")
		failOnError(err, "Failed to read affinities.json", false)
		err = json.Unmarshal(data, &affinities)
		failOnError(err, "Failed to unmarshal affinities", false)

		for worker, aff := range affinities {
			affJSON, _ := json.Marshal(aff)
			rdb.Set(ctx, "affinity_"+worker, affJSON, 0)
		}

		for _, w := range workers {
			status := map[string]interface{}{
				"load":           0.0,
				"current_task":   "",
				"energy_cost":    0.0,
				"free_mem":       1024.0,
				"cpu_usage":      0.0,
				"last_heartbeat": time.Now().Unix(),
				"recent_types":   []string{}, // New: for batching
			}
			setToRedis("status_"+w, status)
		}

		// Declare queues for all workers in manager mode
		for _, w := range workers {
			_, err = ch.QueueDeclare(w, true, false, false, false, nil)
			failOnError(err, "Failed to declare queue for "+w, true)
			err = ch.QueueBind(w, w, "ai_tasks", false, nil)
			failOnError(err, "Failed to bind queue for "+w, true)
		}
	}
	// New: Load avgPowerPerType from Redis if exists
	for _, tt := range taskTypes {
		avgPowerJSON, _ := rdb.Get(ctx, "avg_power_"+tt).Result()
		if avgPowerJSON != "" {
			var data map[string]float64
			json.Unmarshal([]byte(avgPowerJSON), &data)
			avgPowerPerType[tt] = data["avg"]
			countPerType[tt] = int(data["count"])
		}
	}
}

func loadTaskConfig() {
	data, err := os.ReadFile(configFile)
	if err != nil {
		panic(fmt.Sprintf("Failed to read %s: %v", configFile, err))
	}
	err = json.Unmarshal(data, &taskConfig)
	if err != nil {
		panic(fmt.Sprintf("Failed to unmarshal config: %v", err))
	}
}

// Group 2: Redis and Cache Utilities
func getFromRedis[T any](key string) (T, error) {
	var val T
	jsonStr, err := rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		return val, err
	} else if err != nil {
		return val, err
	}
	err = json.Unmarshal([]byte(jsonStr), &val)
	return val, err
}

func setToRedis[T any](key string, val T) error {
	jsonBytes, err := json.Marshal(val)
	if err != nil {
		return err
	}
	return rdb.Set(ctx, key, jsonBytes, 0).Err()
}

func loadAffinities(taskType string) map[int]float64 {
	cacheKey := "aff_" + taskType
	if cached, found := affCache.Get(cacheKey); found {
		return cached.(map[int]float64)
	}
	affMap := make(map[int]float64)
	for i, worker := range workers {
		aff, _ := getFromRedis[map[string]float64]("affinity_" + worker)
		affMap[i] = aff[taskType]
	}
	affCache.Set(cacheKey, affMap, cache.DefaultExpiration)
	return affMap
}

func loadStatus() map[string]map[string]interface{} {
	cacheKey := "status_all"
	if cached, found := statusCache.Get(cacheKey); found {
		return cached.(map[string]map[string]interface{})
	}
	statusMap := make(map[string]map[string]interface{})
	for _, worker := range workers {
		status, _ := getFromRedis[map[string]interface{}]("status_" + worker)
		statusMap[worker] = status
	}
	statusCache.Set(cacheKey, statusMap, cache.DefaultExpiration)
	return statusMap
}

// Group 3: Error Handling and Utilities
func failOnError(err error, msg string, retry bool) {
	if err != nil {
		fmt.Printf("%s: %s\n", msg, err)
		if retry {
			for attempts := 0; attempts < 5; attempts++ { // Retry max 5 lần
				time.Sleep(5 * time.Second) // Retry connect nếu là conn err
				if strings.Contains(msg, "connect") || strings.Contains(msg, "channel") {
					var cerr error
					conn, cerr = amqp.Dial(fmt.Sprintf("amqp://%s:%s@rabbitmq:5672/", rabbitUser, rabbitPass))
					if cerr == nil {
						ch, cerr = conn.Channel()
						if cerr == nil {
							cerr = ch.ExchangeDeclare("ai_tasks", "direct", true, false, false, false, nil)
							if cerr == nil && *mode == "manager" {
								for _, w := range workers {
									// Re-declare queues
									ch.QueueDeclare(w, true, false, false, false, nil)
									ch.QueueBind(w, w, "ai_tasks", false, nil)
								}
							}
							if cerr == nil {
								return // Success
							}
						}
					}
					err = cerr // Update err for next print
					fmt.Printf("Retry %d failed: %s\n", attempts+1, err)
				}
			}
			panic(fmt.Sprintf("%s: max retries exceeded", msg))
		} else {
			panic(fmt.Sprintf("%s: %s", msg, err))
		}
	}
}

func rouletteWheel(probs []float64) int {
	r := rand.Float64()
	sum := 0.0
	for i, p := range probs {
		sum += p
		if r <= sum {
			return i
		}
	}
	return len(probs) - 1
}

func getRecentTypes(worker string) []string {
	status, _ := getFromRedis[map[string]interface{}]("status_" + worker)
	if recent, ok := status["recent_types"].([]interface{}); ok {
		types := make([]string, len(recent))
		for i, v := range recent {
			types[i] = v.(string)
		}
		return types
	}
	return []string{}
}

func calculateAvgComplexity() float64 {
	totalComp := 0.0
	count := 0.0
	for _, w := range workers {
		completedJSON, _ := rdb.LRange(ctx, "completed_tasks_"+w, 0, -1).Result()
		for _, tj := range completedJSON {
			var t map[string]interface{}
			json.Unmarshal([]byte(tj), &t)
			totalComp += t["complexity"].(float64)
			count++
		}
	}
	if count == 0 {
		return 50.0
	}
	return totalComp / count
}

// Group 4: Optimization Algorithms (PSO, ACO, GA)
type gaIndividual struct {
	alpha, beta, rho float64
}

func tournament(pop []gaIndividual, fit func(gaIndividual) float64) gaIndividual {
	cand1, cand2 := pop[rand.Intn(len(pop))], pop[rand.Intn(len(pop))]
	if fit(cand1) > fit(cand2) {
		return cand1
	}
	return cand2
}

func bestInPop(pop []gaIndividual, fit func(gaIndividual) float64) gaIndividual {
	best := pop[0]
	bestFit := fit(pop[0])
	for _, ind := range pop {
		f := fit(ind)
		if f > bestFit {
			bestFit = f
			best = ind
		}
	}
	return best
}

func heuristic(affMap map[int]float64, workerIdx int, taskType string, complexity, cpuReq, memReq, estTime, preTime float64, projectedLoads map[int]float64) float64 {
	worker := workers[workerIdx]
	statusMap := loadStatus()
	status, ok := statusMap[worker]
	if !ok {
		return 0.0001
	}
	projectedLoad := projectedLoads[workerIdx]
	freeMem := status["free_mem"].(float64)
	cpuUsage := status["cpu_usage"].(float64)
	aff := affMap[workerIdx]
	affBoosted := math.Pow(aff, 2.5)
	if aff < 0.3 && projectedLoad < 0.4 { // Mở rộng threshold từ 0.1/0.3 -> 0.3/0.4
		affBoosted = math.Pow(0.3, 2.5)           // Boost lên 0.3 thay vì 0.1
		affBoosted *= (1 + (0.4-projectedLoad)*5) // Stronger multiplier (từ *3 -> *5)
	}
	transFactor := rand.Float64()*8 + 2
	score := affBoosted / complexity * math.Pow(1-projectedLoad, 2.3) * ((100 - cpuUsage) / cpuReq) * (freeMem / memReq) / (1 + transFactor/100)
	if freeMem < memReq {
		score *= math.Max(0, freeMem/memReq*0.7)
	}
	if (100 - cpuUsage) < cpuReq {
		score *= math.Max(0, (100-cpuUsage)/cpuReq*0.7)
	}
	if aff < 0.6 {
		score *= 0.5
	}
	// Load penalty
	if projectedLoad > 0.8 {
		score *= 0.05 // Hard cap
	} else if projectedLoad > 0.7 {
		score *= 0.2
	}
	estPower := estTime * workerPowerFactor[worker] * (complexity / 100.0) // complexity ảnh hưởng power
	avgPowerPerTask := 0.0
	totalPowerAll := 0.0
	totalAssignedAll := 0.0
	for _, w := range workers {
		totalPowerAll += totalPowerMap[w]
		totalAssignedAll += assignedCounts[w]
	}
	if totalAssignedAll > 0 {
		avgPowerPerTask = totalPowerAll / totalAssignedAll
	}
	powerRatio := estPower / (avgPowerPerTask + 1e-6) // tránh chia 0
	powerPenalty := 1.0
	if powerRatio > 1.3 {
		powerPenalty = 0.25 // mạnh penalty nếu power cao hơn 1.5x avg
	} else if powerRatio > 1.1 {
		powerPenalty = 0.5 // nhẹ
	}
	score *= powerPenalty
	// Boost cho worker power thấp (NPU/TPU)
	if workerPowerFactor[worker] < 0.7 && estPower < avgPowerPerTask*0.8 {
		score *= 1.5 // boost tiết kiệm năng lượng
	}
	// Batching bonus (giữ nguyên nhưng tăng window)
	recent := getRecentTypes(worker)
	countMatch := 0
	for _, t := range recent {
		if t == taskType {
			countMatch++
		}
	}
	if countMatch > 0 {
		estTimeAdjusted := estTime - (preTime * 0.7 * float64(countMatch) / 5)
		score *= (1 + 0.5*float64(countMatch)/5)
		score /= (estTimeAdjusted / estTime)
	}
	// Anti-bias & anti-monopoly (giữ + penalty affinity cao)
	totalAssigned := 0.0
	for _, count := range assignedCounts {
		totalAssigned += count
	}
	if totalAssigned > 0 {
		assignedThis := assignedCounts[worker]
		proportion := assignedThis / totalAssigned
		var penalty, boost float64
		if proportion > 0.25 {
			penalty = 1.0 / (1 + (proportion-0.3)*3.5)
		} else {
			penalty = 1.0
		}
		boost = 1.0 + (0.3-proportion)*2
		score *= penalty * boost
	}
	// Anti-monopoly affinity
	affMapAll := loadAffinities(taskType)
	maxAffAll := 0.0
	for _, a := range affMapAll {
		if a > maxAffAll {
			maxAffAll = a
		}
	}
	if aff > 0.8 || aff >= maxAffAll*0.95 {
		score *= 0.4
	}
	if score < 0.0001 {
		score = 0.0001
	}
	// New: Add random exploration to avoid stuck
	exploration := rand.Float64()*0.2 + 0.9 // 0.9-1.1 random factor
	score *= exploration
	return score
}

func psoPrior(affMap map[int]float64, taskType string, complexity, cpuReq, memReq, estTime, preTime float64, projectedLoads map[int]float64) []float64 {
	type particle struct {
		pos, vel, pbest, pbestScore float64
	}
	numParticles := 5
	iterations := 3
	particles := make([]particle, numParticles)
	gbest := 0.0
	gbestScore := -1.0
	for i := range particles {
		particles[i] = particle{
			pos:        rand.Float64() * 4,
			vel:        rand.Float64()*2 - 1,
			pbest:      rand.Float64() * 4,
			pbestScore: -1,
		}
		s := heuristic(affMap, int(math.Round(particles[i].pos)), taskType, complexity, cpuReq, memReq, estTime, preTime, projectedLoads)
		particles[i].pbest = particles[i].pos
		particles[i].pbestScore = s
		if s > gbestScore {
			gbestScore = s
			gbest = particles[i].pos
		}
	}
	w, c1, c2 := 0.5, 1.5, 1.5
	for iter := 0; iter < iterations; iter++ {
		for i := range particles {
			r1, r2 := rand.Float64(), rand.Float64()
			particles[i].vel = w*particles[i].vel + c1*r1*(particles[i].pbest-particles[i].pos) + c2*r2*(gbest-particles[i].pos)
			particles[i].pos += particles[i].vel
			if particles[i].pos < 0 {
				particles[i].pos = 0
			} else if particles[i].pos > 4 {
				particles[i].pos = 4
			}
			s := heuristic(affMap, int(math.Round(particles[i].pos)), taskType, complexity, cpuReq, memReq, estTime, preTime, projectedLoads)
			if s > particles[i].pbestScore {
				particles[i].pbest = particles[i].pos
				particles[i].pbestScore = s
			}
			if s > gbestScore {
				gbestScore = s
				gbest = particles[i].pos
			}
		}
	}
	initialPheromone := make([]float64, len(workers))
	for i := range initialPheromone {
		initialPheromone[i] = 1.0
	}
	sort.Slice(particles, func(i, j int) bool {
		return particles[i].pbestScore > particles[j].pbestScore
	})
	for i := 0; i < 5 && i < len(particles); i++ {
		bestIdx := int(math.Round(particles[i].pbest))
		initialPheromone[bestIdx] += particles[i].pbestScore
	}
	return initialPheromone
}

func acoAssign(affMap map[int]float64, taskType string, initialPheromone []float64, alpha, beta, rho float64, complexity, cpuReq, memReq, estTime, preTime float64, projectedLoads map[int]float64) int {
	numAnts := 5
	iterations := 5
	copy(pheromone, initialPheromone)
	minPhero, maxPhero := 0.1, 5.0
	for it := 0; it < iterations; it++ {
		probabilities := make([]float64, len(workers))
		total := 0.0
		for i := range workers {
			h := heuristic(affMap, i, taskType, complexity, cpuReq, memReq, estTime, preTime, projectedLoads)
			prob := math.Pow(pheromone[i], alpha) * math.Pow(h, beta)
			probabilities[i] = prob
			total += prob
		}
		for i := range probabilities {
			probabilities[i] /= total
		}
		for ant := 0; ant < numAnts; ant++ {
			choice := rouletteWheel(probabilities)
			maxScore := 0.0
			for i := range workers {
				s := heuristic(affMap, i, taskType, complexity, cpuReq, memReq, estTime, preTime, projectedLoads)
				if s > maxScore {
					maxScore = s
				}
			}
			delta := heuristic(affMap, choice, taskType, complexity, cpuReq, memReq, estTime, preTime, projectedLoads) / maxScore
			pheromone[choice] = (1-rho)*pheromone[choice] + delta
			pheromone[choice] = math.Max(minPhero, math.Min(maxPhero, pheromone[choice]))
		}
	}
	bestIdx := 0
	bestVal := -1.0
	for i := range workers {
		h := heuristic(affMap, i, taskType, complexity, cpuReq, memReq, estTime, preTime, projectedLoads)
		val := pheromone[i] * h
		if val > bestVal {
			bestVal = val
			bestIdx = i
		}
	}
	// New: Evaporate extra if chosen high-load, boost idle
	chosenWorker := workers[bestIdx]
	statusMap := loadStatus()
	_ = statusMap[chosenWorker]
	projectedLoad := projectedLoads[bestIdx]
	if projectedLoad > 0.5 {
		pheromone[bestIdx] *= (1 - rho*1.5) // Stronger evaporation cho high-load
		pheromone[bestIdx] = math.Max(minPhero, pheromone[bestIdx])
	}
	// Boost pheromone cho idle workers (low projectedLoad)
	for i := range workers {
		if projectedLoads[i] < 0.2 && i != bestIdx {
			pheromone[i] += 0.5 * rho // Add delta to encourage assignment
			pheromone[i] = math.Min(maxPhero, pheromone[i])
		}
	}
	return bestIdx
}

func calculateFitness(ind gaIndividual) float64 {
	totalScore := 0.0
	count := 0.0
	loads := make([]float64, 0)
	avgLatency := 0.0
	maxLoad, minLoad := 0.0, 1.0
	for _, w := range workers {
		status, _ := getFromRedis[map[string]interface{}]("status_" + w)
		l := status["load"].(float64)
		loads = append(loads, l)
		if l > maxLoad {
			maxLoad = l
		}
		if l < minLoad || minLoad == 1.0 {
			minLoad = l
		}
		completedJSON, _ := rdb.LRange(ctx, "completed_tasks_"+w, 0, -1).Result()
		for _, tj := range completedJSON {
			var t map[string]interface{}
			json.Unmarshal([]byte(tj), &t)
			tt := t["type"].(string)
			affMap := loadAffinities(tt)
			comp := t["complexity"].(float64)
			cpu := t["cpu_req"].(float64)
			mem := t["mem_req"].(float64)
			estTime := t["estimated_process_time"].(float64)
			preTime := t["pre_time"].(float64) // Add for heuristic
			idx := sort.SearchStrings(workers, w)
			score := heuristic(affMap, idx, tt, comp, cpu, mem, estTime, preTime, nil)
			totalScore += score
			avgLatency += t["process_time"].(float64)
			count++
		}
	}
	if count == 0 {
		return 0.0
	}
	avgScore := totalScore / count
	avgLatency /= count
	var sumSq float64
	avgLoad := 0.0
	for _, l := range loads {
		avgLoad += l
	}
	avgLoad /= float64(len(loads))
	for _, l := range loads {
		sumSq += math.Pow(l-avgLoad, 2)
	}
	variance := sumSq / float64(len(loads))
	totalPowerAll := 0.0
	totalTasksAll := 0.0
	for _, w := range workers {
		totalPowerAll += totalPowerMap[w]
		totalTasksAll += assignedCounts[w] // approx
	}
	powerEfficiency := 1.0
	if totalTasksAll > 0 {
		avgPower := totalPowerAll / totalTasksAll
		powerEfficiency = 1.0 / (1 + avgPower/800.0) // normalize, giả sử avgPower ~1000 đơn vị
	}
	fitness := avgScore * (1 - variance*1.5) * (1 / avgLatency) * powerEfficiency * 1.3
	if minLoad > 0 {
		fitness *= (1 - (maxLoad/minLoad)*0.5)
	}
	return fitness
}

func updateParams() {
	totalCompleted := 0
	for _, w := range workers {
		totalCompleted += int(rdb.LLen(ctx, "completed_tasks_"+w).Val())
	}
	if totalCompleted < 5 || totalCompleted%5 != 0 { // New: Update every 5 completed (faster)
		return
	}
	popSize := 20
	if totalCompleted > 100 {
		popSize = 30
	}
	generations := 20
	if totalCompleted > 100 {
		generations = 30
	}
	crossoverRate := 0.8
	mutationRate := 0.1
	population := make([]gaIndividual, popSize)
	for i := range population {
		population[i] = gaIndividual{
			alpha: rand.Float64()*2 + 0.5,
			beta:  rand.Float64()*3 + 1.0,
			rho:   rand.Float64()*0.2 + 0.05,
		}
	}
	fitness := calculateFitness
	for gen := 0; gen < generations; gen++ {
		newPop := make([]gaIndividual, popSize)
		for i := 0; i < popSize; i += 2 {
			parent1 := tournament(population, fitness)
			parent2 := tournament(population, fitness)
			child1, child2 := parent1, parent2
			if rand.Float64() < crossoverRate {
				weight := rand.Float64()
				child1.alpha = weight*parent1.alpha + (1-weight)*parent2.alpha
				child1.beta = weight*parent1.beta + (1-weight)*parent2.beta
				child1.rho = weight*parent1.rho + (1-weight)*parent2.rho
				child2.alpha = (1-weight)*parent1.alpha + weight*parent2.alpha
				child2.beta = (1-weight)*parent1.beta + weight*parent2.beta
				child2.rho = (1-weight)*parent1.rho + weight*parent2.rho
			}
			newPop[i] = child1
			if i+1 < popSize {
				newPop[i+1] = child2
			}
		}
		mutRate := mutationRate * (1 - float64(gen)/float64(generations))
		for i := range newPop {
			if rand.Float64() < mutRate {
				newPop[i].alpha += rand.Float64()*0.4 - 0.2
				if newPop[i].alpha < 0.5 {
					newPop[i].alpha = 0.5
				}
				if newPop[i].alpha > 3 {
					newPop[i].alpha = 3
				}
				newPop[i].beta += rand.Float64()*0.6 - 0.3
				if newPop[i].beta < 1 {
					newPop[i].beta = 1
				}
				if newPop[i].beta > 5 {
					newPop[i].beta = 5
				}
				newPop[i].rho += rand.Float64()*0.1 - 0.05
				if newPop[i].rho < 0.05 {
					newPop[i].rho = 0.05
				}
				if newPop[i].rho > 0.3 {
					newPop[i].rho = 0.3
				}
			}
		}
		population = newPop
	}
	best := bestInPop(population, fitness)
	alpha = best.alpha
	beta = best.beta
	rho = best.rho
	// New: Increase rho if variance high
	loads := make([]float64, len(workers))
	for i, w := range workers {
		status, _ := getFromRedis[map[string]interface{}]("status_" + w)
		loads[i] = status["load"].(float64)
	}
	avgLoad := 0.0
	for _, l := range loads {
		avgLoad += l
	}
	avgLoad /= float64(len(loads))
	var sumSq float64
	for _, l := range loads {
		sumSq += math.Pow(l-avgLoad, 2)
	}
	variance := sumSq / float64(len(loads))
	if variance > 0.1 {
		rho = math.Min(0.4, rho+0.1) // Increase evaporation for diversity
	}
	for _, w := range workers {
		status, _ := getFromRedis[map[string]interface{}]("status_" + w)
		load := status["load"].(float64)
		aff, _ := getFromRedis[map[string]float64]("affinity_" + w)
		avgAff := 0.0
		for _, v := range aff {
			avgAff += v
		}
		avgAff /= float64(len(aff))
		if load < 0.1 && avgAff < 0.1 {
			// New: Reset if idle and low aff
			for tt := range aff {
				aff[tt] = 0.3
			}
			setToRedis("affinity_"+w, aff)
		}
		completedJSON, _ := rdb.LRange(ctx, "completed_tasks_"+w, 0, -1).Result()
		if len(completedJSON) == 0 {
			continue
		}
		typeCounts := make(map[string]int)
		typeTimes := make(map[string]float64)
		typeExpected := make(map[string]float64) // New: for avg expected
		for _, tj := range completedJSON {
			var t map[string]interface{}
			json.Unmarshal([]byte(tj), &t)
			tt := t["type"].(string)
			pt := t["process_time"].(float64)
			typeCounts[tt]++
			typeTimes[tt] += pt
			typeExpected[tt] += t["estimated_process_time"].(float64) // New
		}
		for tt := range aff {
			if count := typeCounts[tt]; count > 0 {
				avgTime := typeTimes[tt] / float64(count)
				avgExpected := typeExpected[tt] / float64(count) // New
				if avgTime < avgExpected*0.9 {                   // New: Lower threshold to 0.9/1.1 for faster update
					aff[tt] = math.Min(aff[tt]+0.2, 1.0) // New: Larger step 0.2
				} else if avgTime > avgExpected*1.1 {
					aff[tt] = math.Max(aff[tt]-0.2, 0.0) // New: Larger step -0.2
				}
			}
		}
		setToRedis("affinity_"+w, aff)
	}
	syncAffinitiesToFile()
}

// Group 5: Task Assignment and Management
func assignTaskWithPSOACOGA(task map[string]interface{}) string {
	taskID := fmt.Sprintf("%v", task["id"])
	exists, _ := rdb.SIsMember(ctx, "assigned_tasks", taskID).Result()
	if exists {
		return ""
	}
	taskType := task["type"].(string)
	res := task["resolution"].(string)
	durStr := fmt.Sprintf("%.0f", task["duration"].(float64))
	fpsStr := fmt.Sprintf("%.0f", task["fps"].(float64)) // Assume fps in task
	entry, ok := taskConfig[taskType][res][durStr][fpsStr]
	if !ok {
		fmt.Printf("Config not found for %s %s %s %s, fallback default\n", taskType, res, durStr, fpsStr)
		return ""
	}
	complexity := entry["complexity"]
	cpuReq := entry["cpu_req"]
	memReq := entry["mem_req"]
	estTime := entry["estimated_process_time"]
	preTime := entry["pre_time"] // New
	task["complexity"] = complexity
	task["cpu_req"] = cpuReq
	task["mem_req"] = memReq
	task["estimated_process_time"] = estTime
	task["pre_time"] = preTime                                     // New for batching in worker
	task["assign_time"] = time.Now().Format("2006-01-02 15:04:05") // New: Assign timestamp
	affMap := loadAffinities(taskType)
	avgComplexity := calculateAvgComplexity()
	projectedLoads := make(map[int]float64)
	projectedPowers := make(map[int]float64) // Thêm mới
	totalPowerAll := 0.0
	totalAssignedAll := 0.0
	for _, w := range workers {
		totalPowerAll += totalPowerMap[w]
		totalAssignedAll += assignedCounts[w]
	}
	for i, worker := range workers {
		if ch.IsClosed() {
			fmt.Printf("Channel closed, reconnecting...\n")
			failOnError(fmt.Errorf("closed"), "Reopen channel", true) // Sử dụng failOnError để reconnect
		}
		qInspect, err := ch.QueueInspect(worker)
		queued := 0.0
		if err == nil {
			queued = float64(qInspect.Messages)
		} else {
			fmt.Printf("QueueInspect failed for %s: %v (assuming queued=0)\n", worker, err)
		}
		statusMap := loadStatus()
		status, ok := statusMap[worker]
		if !ok {
			projectedLoads[i] = 0.0
			continue
		}
		currentLoad := status["load"].(float64)
		// New: Adjust for batching
		recent := getRecentTypes(worker)
		countMatch := 0
		for _, t := range recent {
			if t == taskType {
				countMatch++
			}
		}
		avgCompAdj := avgComplexity
		if countMatch > 0 {
			avgCompAdj *= (1 - 0.1*float64(countMatch)/3)
		}
		projectedLoads[i] = currentLoad + (queued * avgCompAdj / 100.0)
		estPower := estTime * workerPowerFactor[worker] * (complexity / 100.0)
		projectedPowers[i] = estPower
	}
	// Removed: Force assign to idle (theo yêu cầu, để nổi bật thuật toán PSO-ACO-GA)
	initialPheromone := psoPrior(affMap, taskType, complexity, cpuReq, memReq, estTime, preTime, projectedLoads)
	acoBestIdx := acoAssign(affMap, taskType, initialPheromone, alpha, beta, rho, complexity, cpuReq, memReq, estTime, preTime, projectedLoads)
	worker := workers[acoBestIdx]
	ts := time.Now().Format("2006-01-02 15:04:05") // New: Timestamp
	fmt.Printf("[%s] [ASSIGN] Task ID=%v (type=%s) ASSIGNED to %s\n", ts, task["id"], taskType, worker)
	err := rdb.Watch(ctx, func(tx *redis.Tx) error {
		status, _ := getFromRedis[map[string]interface{}]("status_" + worker)
		newLoad := math.Min(status["load"].(float64)+(complexity/100.0), 1.0)
		status["load"] = newLoad
		workerLoadMap[worker] = newLoad
		workerLoad.WithLabelValues(worker).Set(newLoad)
		status["cpu_usage"] = math.Min(status["cpu_usage"].(float64)+cpuReq, 100.0)
		status["free_mem"] = math.Max(status["free_mem"].(float64)-memReq, 0.0)
		// New: Update recent_types
		recentIntf, ok := status["recent_types"].([]interface{})
		recentStr := make([]string, 0)
		if ok {
			for _, v := range recentIntf {
				recentStr = append(recentStr, v.(string))
			}
		}
		recentStr = append(recentStr, taskType)
		if len(recentStr) > 5 {
			recentStr = recentStr[len(recentStr)-5:]
		}
		status["recent_types"] = recentStr
		return setToRedis("status_"+worker, status)
	}, "status_"+worker)
	if err != nil {
		fmt.Printf("Error updating status: %v\n", err)
	}
	rdb.SAdd(ctx, "assigned_tasks", taskID)
	exportAssignment(task, worker)
	exportWorkerStatus()
	// Removed: exportVariables()
	// New: Log reason
	logAssignmentReason(task, worker, affMap, projectedLoads, pheromone)
	if worker == "" {
		fmt.Printf("[MANAGER] Failed to assign task ID=%v\n", task["id"])
		return ""
	}
	return worker
}

func logAssignmentReason(task map[string]interface{}, worker string, affMap map[int]float64, projectedLoads map[int]float64, pheromone []float64) {
	taskID := task["id"]
	taskType := task["type"].(string)
	complexity := task["complexity"].(float64)
	cpuReq := task["cpu_req"].(float64)
	memReq := task["mem_req"].(float64)
	estTime := task["estimated_process_time"].(float64)
	preTime := task["pre_time"].(float64) // New
	idx := sort.SearchStrings(workers, worker)
	score := heuristic(affMap, idx, taskType, complexity, cpuReq, memReq, estTime, preTime, projectedLoads)
	aff := affMap[idx]
	projectedLoad := projectedLoads[idx]
	recent := getRecentTypes(worker)
	countMatch := 0
	for _, t := range recent {
		if t == taskType {
			countMatch++
		}
	}
	totalAssigned := 0.0
	for _, count := range assignedCounts {
		totalAssigned += count
	}
	assignedThis := assignedCounts[worker]
	proportionAssigned := 0.0
	if totalAssigned > 0 {
		proportionAssigned = assignedThis / totalAssigned
	}
	fmt.Printf("[ASSIGN_LOG] Task ID=%v (type=%s) assigned to %s:\n", taskID, taskType, worker)
	fmt.Printf(" - Heuristic score: %.4f\n", score)
	fmt.Printf(" - Affinity: %.4f\n", aff)
	fmt.Printf(" - Projected load: %.4f\n", projectedLoad)
	fmt.Printf(" - Pheromone: %.4f\n", pheromone[idx])
	fmt.Printf(" - Batching matches (last 3): %d (bonus applied if >0)\n", countMatch)
	fmt.Printf(" - Anti-bias: Proportion assigned %.4f (penalty if high)\n", proportionAssigned)
	fmt.Printf(" - Complexity: %.2f, Est time: %.2f s, CPU req: %.2f%%, Mem req: %.2f MB\n", complexity, estTime, cpuReq, memReq)
}

func flushWorkerTasks(worker string, wtasks []map[string]interface{}) {
	if len(wtasks) == 0 {
		return
	}
	// Sort stable by type
	sort.SliceStable(wtasks, func(i, j int) bool {
		return wtasks[i]["type"].(string) < wtasks[j]["type"].(string)
	})
	// Publish in sorted order
	for _, task := range wtasks {
		taskJSON, _ := json.Marshal(task)
		err := ch.Publish("ai_tasks", worker, false, false, amqp.Publishing{ContentType: "application/json", Body: taskJSON})
		if err != nil {
			fmt.Printf("Failed to publish task ID=%v to %s: %v\n", task["id"], worker, err)
		}
	}
	fmt.Printf("[MANAGER] Flushed %d sorted tasks to worker %s\n", len(wtasks), worker)
}

// Group 6: Export Functions
func exportCompletedOrder() {
	completedOrder := make(map[string][]map[string]interface{})
	for _, worker := range workers {
		completedJSON, err := rdb.LRange(ctx, "completed_tasks_"+worker, 0, -1).Result()
		if err != nil {
			fmt.Printf("Failed to get completed tasks for %s: %v\n", worker, err)
			continue
		}
		order := make([]map[string]interface{}, 0)
		for _, tj := range completedJSON {
			var t map[string]interface{}
			if err := json.Unmarshal([]byte(tj), &t); err == nil {
				if id, okId := t["id"]; okId {
					if taskType, okType := t["type"]; okType {
						item := map[string]interface{}{
							"id":   id,
							"type": taskType, // Kèm theo tên task (type)
						}
						if assignTime, ok := t["assign_time"]; ok {
							item["assign_time"] = assignTime // New
						}
						if completeTime, ok := t["complete_time"]; ok {
							item["complete_time"] = completeTime // New
						}
						order = append(order, item)
					}
				}
			}
		}
		completedOrder[worker] = order
	}
	data, err := json.MarshalIndent(completedOrder, "", " ")
	if err != nil {
		fmt.Printf("Failed to marshal completed order: %v\n", err)
		return
	}
	err = os.WriteFile("completed_order.json", data, 0644)
	if err != nil {
		fmt.Printf("Failed to write completed_order.json: %v\n", err)
	} else {
		fmt.Println("Exported completed task order (with types) to completed_order.json")
	}
}

func exportWorkerStatus() {
	statusAll := make(map[string]map[string]interface{})
	for _, worker := range workers {
		status, err := getFromRedis[map[string]interface{}]("status_" + worker)
		if err != nil {
			continue
		}
		completedList := make([]map[string]interface{}, 0)
		completedJSON, err := rdb.LRange(ctx, "completed_tasks_"+worker, 0, -1).Result()
		if err == nil {
			for _, tj := range completedJSON {
				var t map[string]interface{}
				if json.Unmarshal([]byte(tj), &t) == nil {
					completedList = append(completedList, t)
				}
			}
		}
		status["completed_tasks"] = completedList
		statusAll[worker] = status
		if load, ok := status["load"].(float64); ok {
			workerLoad.WithLabelValues(worker).Set(load)
			workerLoadMap[worker] = load
		}
		if energy, ok := status["energy_cost"].(float64); ok {
			totalPowerMap[worker] = energy
		}
	}
	data, _ := json.MarshalIndent(statusAll, "", " ")
	os.WriteFile("worker_status.json", data, 0644)
	exportCompletedOrder()
}

func exportAssignment(task map[string]interface{}, worker string) {
	assignment := map[string]interface{}{"id": task["id"], "type": task["type"], "assigned_to": worker}
	var assignments []map[string]interface{}
	if data, err := os.ReadFile("assignments.json"); err == nil {
		json.Unmarshal(data, &assignments)
	}
	assignments = append(assignments, assignment)
	newData, _ := json.MarshalIndent(assignments, "", " ")
	os.WriteFile("assignments.json", newData, 0644)
	tasksAssigned.WithLabelValues(worker).Inc()
	assignedCounts[worker]++ // New: Increment internal count
}

func syncAffinitiesToFile() {
	affAll := make(map[string]map[string]float64)
	for _, w := range workers {
		aff, _ := getFromRedis[map[string]float64]("affinity_" + w)
		affAll[w] = aff
	}
	data, _ := json.MarshalIndent(affAll, "", " ")
	os.WriteFile("affinities.json", data, 0644)
}

// New: Export Metrics Function
func exportMetrics() {
	metricsData := make(map[string]interface{})
	queueLenTotal := 0.0
	for _, w := range workers {
		metricsData[fmt.Sprintf("tasks_assigned_%s", w)] = assignedCounts[w]
		metricsData[fmt.Sprintf("task_errors_%s", w)] = taskErrorsMap[w]
		metricsData[fmt.Sprintf("total_power_%s", w)] = totalPowerMap[w]
		metricsData[fmt.Sprintf("worker_load_%s", w)] = workerLoadMap[w]
		// For avg_process_time, calculate from completed tasks
		avgProcessTime := 0.0
		countProcess := 0.0
		completedJSON, _ := rdb.LRange(ctx, "completed_tasks_"+w, 0, -1).Result()
		for _, tj := range completedJSON {
			var t map[string]interface{}
			json.Unmarshal([]byte(tj), &t)
			avgProcessTime += t["process_time"].(float64)
			countProcess++
		}
		if countProcess > 0 {
			avgProcessTime /= countProcess
		}
		metricsData[fmt.Sprintf("avg_process_time_%s", w)] = avgProcessTime
		// Queue length per worker
		qInspect, err := ch.QueueInspect(w)
		if err == nil {
			queueLenTotal += float64(qInspect.Messages)
		}
	}
	// Custom metrics
	loads := make([]float64, len(workers))
	avgLatency := 0.0
	count := 0.0
	avgScore := 0.0
	minLoad, maxLoad := math.MaxFloat64, 0.0
	for i, w := range workers {
		status, _ := getFromRedis[map[string]interface{}]("status_" + w)
		load := status["load"].(float64)
		loads[i] = load
		if load < minLoad {
			minLoad = load
		}
		if load > maxLoad {
			maxLoad = load
		}
		completedJSON, _ := rdb.LRange(ctx, "completed_tasks_"+w, 0, -1).Result()
		for _, tj := range completedJSON {
			var t map[string]interface{}
			json.Unmarshal([]byte(tj), &t)
			avgLatency += t["process_time"].(float64)
			// Approx score: use 1.0 or calculate heuristic if needed
			tt := t["type"].(string)
			affMap := loadAffinities(tt)
			comp := t["complexity"].(float64)
			cpu := t["cpu_req"].(float64)
			mem := t["mem_req"].(float64)
			estTime := t["estimated_process_time"].(float64)
			preTime := t["pre_time"].(float64) // New for heuristic
			idx := sort.SearchStrings(workers, w)
			score := heuristic(affMap, idx, tt, comp, cpu, mem, estTime, preTime, nil) // nil projectedLoads for approx
			avgScore += score
			count++
		}
	}
	if count > 0 {
		avgLatency /= count
		avgScore /= count
	}
	avgLoad := 0.0
	for _, l := range loads {
		avgLoad += l
	}
	avgLoad /= float64(len(loads))
	var sumSq float64
	for _, l := range loads {
		sumSq += math.Pow(l-avgLoad, 2)
	}
	variance := sumSq / float64(len(loads))
	metricsData["load_variance"] = variance
	metricsData["avg_latency"] = avgLatency
	metricsData["avg_affinity_score"] = avgScore
	biasRatio := 0.0
	if minLoad > 0 {
		biasRatio = maxLoad / minLoad
	}
	metricsData["bias_ratio"] = biasRatio
	metricsData["queue_length"] = queueLenTotal
	queueLength.Set(queueLenTotal)
	// Throughput approx: total completed / time (seconds)
	throughput := 0.0
	elapsed := time.Since(startTime).Seconds()
	if elapsed > 0 {
		throughput = count / elapsed
	}
	metricsData["throughput"] = throughput
	data, err := json.MarshalIndent(metricsData, "", " ")
	if err != nil {
		fmt.Printf("Failed to marshal metrics: %v\n", err)
		return
	}
	err = os.WriteFile("metrics.json", data, 0644)
	if err != nil {
		fmt.Printf("Failed to write metrics.json: %v\n", err)
	} else {
		fmt.Println("Exported metrics to metrics.json")
	}
}

// Group 7: Manager and Worker Main Functions
var startTime = time.Now() // For throughput calc in exportMetrics

func managerMain() {
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":2112", nil)
	}()
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		for range ticker.C {
			updateParams()
			exportMetrics() // New: Export metrics periodically
		}
	}()
	go func() {
		for {
			time.Sleep(10 * time.Second)
			for _, w := range workers {
				status, err := getFromRedis[map[string]interface{}]("status_" + w)
				if err != nil {
					continue
				}
				hbVal, ok := status["last_heartbeat"]
				if !ok {
					continue
				}
				lastHB := int64(hbVal.(float64))
				if time.Now().Unix()-lastHB > 30 {
					current := status["current_task"].(string)
					if current != "" {
						fmt.Printf("Worker %s down, reassigning %s\n", w, current)
					}
				}
			}
		}
	}()
	// New: Goroutine for decaying high aff if minLoad low
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		for range ticker.C {
			statusMap := loadStatus()
			minLoad := 1.0
			for _, status := range statusMap {
				load := status["load"].(float64)
				if load < minLoad {
					minLoad = load
				}
			}
			if minLoad < 0.1 { // Có worker rất idle -> decay high aff để cân bằng
				for _, w := range workers {
					aff, _ := getFromRedis[map[string]float64]("affinity_" + w)
					for tt := range aff {
						if aff[tt] > 0.7 {
							aff[tt] = math.Max(aff[tt]-0.05, 0.3) // Decay dần
						}
					}
					setToRedis("affinity_"+w, aff)
				}
				syncAffinitiesToFile()
			}
		}
	}()
	workerTasks := make(map[string][]map[string]interface{})
	taskID := 0
	// Goroutine to flush periodically
	go func() {
		ticker := time.NewTicker(30 * time.Second) // Flush every 30 seconds
		for range ticker.C {
			for worker, wtasks := range workerTasks {
				flushWorkerTasks(worker, wtasks)
				workerTasks[worker] = nil // Clear after flush
			}
			exportMetrics() // New: Export after flush
		}
	}()
	for {
		time.Sleep(time.Duration(rand.Intn(11)+5) * time.Second)
		tt := taskTypes[rand.Intn(len(taskTypes))]
		newTask := map[string]interface{}{
			"id":         taskID,
			"type":       tt,
			"video_seq":  fmt.Sprintf("video_%d.h264", taskID),
			"duration":   durations[rand.Intn(len(durations))],
			"resolution": resolutions[rand.Intn(len(resolutions))],
			"fps":        float64(fpsOptions[rand.Intn(len(fpsOptions))]),
			"cpu_req":    0.0, // Will be set in assign
			"mem_req":    0.0,
		}
		ts := time.Now().Format("2006-01-02 15:04:05") // New: Timestamp
		fmt.Printf("[%s] [CREATE] Task ID=%v (type=%s) CREATED\n", ts, newTask["id"], tt)
		worker := assignTaskWithPSOACOGA(newTask)
		if worker != "" {
			workerTasks[worker] = append(workerTasks[worker], newTask)
		}
		taskID++
	}
}

func workerMain(id string) {
	q, err := ch.QueueDeclare(id, true, false, false, false, nil)
	failOnError(err, "Failed to declare queue", true)
	err = ch.QueueBind(q.Name, id, "ai_tasks", false, nil)
	failOnError(err, "Failed to bind queue", true)
	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	failOnError(err, "Failed to register consumer", true)
	go func() {
		for {
			time.Sleep(10 * time.Second)
			status, _ := getFromRedis[map[string]interface{}]("status_" + id)
			status["last_heartbeat"] = time.Now().Unix()
			setToRedis("status_"+id, status)
		}
	}()
	for d := range msgs {
		var task map[string]interface{}
		err := json.Unmarshal(d.Body, &task)
		if err != nil {
			taskErrors.WithLabelValues(id).Inc()
			taskErrorsMap[id]++
			d.Ack(false)
			continue
		}
		rdb.Watch(ctx, func(tx *redis.Tx) error {
			status, _ := getFromRedis[map[string]interface{}]("status_" + id)
			status["current_task"] = task["type"]
			fmt.Printf("[WORKER %s] STARTED Task ID=%v (type=%s)\n", id, task["id"], task["type"])
			return setToRedis("status_"+id, status)
		}, "status_"+id)
		processTime := calculateProcessTime(task)
		time.Sleep(time.Duration(processTime) * time.Second)
		rdb.Watch(ctx, func(tx *redis.Tx) error {
			status, _ := getFromRedis[map[string]interface{}]("status_" + id) // Dòng tiềm năng lỗi (561:2) - đảm bảo sử dụng status dưới
			complexity := task["complexity"].(float64)
			newLoad := math.Max(0, status["load"].(float64)-(complexity/100.0))
			status["load"] = newLoad
			workerLoadMap[id] = newLoad
			workerLoad.WithLabelValues(id).Set(newLoad)
			status["cpu_usage"] = math.Max(0, status["cpu_usage"].(float64)-task["cpu_req"].(float64))
			status["free_mem"] = math.Min(status["free_mem"].(float64)+task["mem_req"].(float64), 1024.0)
			status["current_task"] = ""
			// Giữ nguyên phần energy: (sử dụng status["cpu_usage"] để tránh lỗi not used)
			energy := float64(processTime) * workerPowerFactor[id] * (status["cpu_usage"].(float64)/100*100 + task["mem_req"].(float64)/1024*50)
			status["energy_cost"] = status["energy_cost"].(float64) + energy
			newPower := totalPowerMap[id] + energy
			totalPowerMap[id] = newPower
			totalPower.WithLabelValues(id).Add(energy)
			return setToRedis("status_"+id, status)
		}, "status_"+id)
		ts := time.Now().Format("2006-01-02 15:04:05") // New: Timestamp
		task["complete_time"] = ts                     // New: Complete timestamp
		fmt.Printf("[%s] [COMPLETE] Task ID=%v (type=%s) COMPLETED in %d seconds by %s\n", ts, task["id"], task["type"], processTime, id)
		d.Ack(false)
		task["process_time"] = float64(processTime)
		taskJSON, _ := json.Marshal(task)
		rdb.RPush(ctx, "completed_tasks_"+id, taskJSON)
		// New: Update avg power for type
		tt := task["type"].(string)
		actualEnergy := float64(processTime) * workerPowerFactor[id] * (task["cpu_req"].(float64)/100*100 + task["mem_req"].(float64)/1024*50) // Recalc energy if needed
		rdb.Watch(ctx, func(tx *redis.Tx) error {
			avgPowerJSON, _ := rdb.Get(ctx, "avg_power_"+tt).Result()
			var avgPower float64
			var count int
			if avgPowerJSON != "" {
				var data map[string]float64
				json.Unmarshal([]byte(avgPowerJSON), &data)
				avgPower = data["avg"]
				count = int(data["count"])
			}
			newCount := count + 1
			newAvg := (avgPower*float64(count) + actualEnergy) / float64(newCount)
			newData := map[string]float64{"avg": newAvg, "count": float64(newCount)}
			jsonBytes, _ := json.Marshal(newData)
			rdb.Set(ctx, "avg_power_"+tt, jsonBytes, 0)
			avgPowerPerType[tt] = newAvg
			countPerType[tt] = newCount
			return nil
		}, "avg_power_"+tt)
		// Tính power_ratio
		avgPower := avgPowerPerType[tt]
		if avgPower == 0 {
			avgPower = actualEnergy
		}
		powerRatio := actualEnergy / avgPower
		// Update affinity
		rdb.Watch(ctx, func(tx *redis.Tx) error {
			aff, _ := getFromRedis[map[string]float64]("affinity_" + id)
			expectedTime := task["estimated_process_time"].(float64)
			timeStep := 0.05
			powerStep := 0.1 // Stronger step for power to emphasize
			if float64(processTime) < expectedTime*0.9 {
				aff[tt] = math.Min(aff[tt]+timeStep, 1.0)
			} else if float64(processTime) > expectedTime*1.1 {
				aff[tt] = math.Max(aff[tt]-timeStep, 0.0)
			}
			// Power adjustment (independent of time)
			if powerRatio < 0.9 { // Tiết kiệm power -> boost
				aff[tt] = math.Min(aff[tt]+powerStep, 1.0)
			} else if powerRatio > 1.1 { // Tốn power -> penalty
				aff[tt] = math.Max(aff[tt]-powerStep, 0.0)
			}
			return setToRedis("affinity_"+id, aff)
		}, "affinity_"+id)
		syncAffinitiesToFile()
		taskProcessTime.WithLabelValues(id).Observe(float64(processTime))
		exportWorkerStatus()
		exportMetrics() // New: Export after task completion
	}
}

// Cần phải chứng minh với 50 tasks cùng loại chạy liên tục thì process time sẽ giảm bao nhiêu %
func calculateProcessTime(task map[string]interface{}) int {
	estTime := task["estimated_process_time"].(float64)
	preTime := task["pre_time"].(float64) // New
	noise := rand.Float64()*0.2 - 0.1
	// New: Apply worker speed multiplier (simulate diff)
	speed := workerSpeed[*workerID]
	estTime *= speed // If speed <1, faster (lower time)
	// New: Batching reduce based on pre_time
	status, _ := getFromRedis[map[string]interface{}]("status_" + *workerID)
	recentIntf, ok := status["recent_types"].([]interface{})
	countMatch := 0
	if ok {
		for _, v := range recentIntf {
			if v.(string) == task["type"].(string) {
				countMatch++
			}
		}
	}
	if countMatch > 0 {
		reduction := preTime * 0.5 * float64(countMatch) / 3
		estTime -= reduction
		if estTime < 0 {
			estTime = 0
		}
	}
	return int(estTime*(1+noise)) + 5
}

// Group 8: Main Entry
func main() {
	if *mode == "manager" {
		managerMain()
	} else if *mode == "worker" && *workerID != "" {
		valid := false
		for _, w := range workers {
			if *workerID == w {
				valid = true
				break
			}
		}
		if !valid {
			panic("Invalid worker ID")
		}
		go func() {
			http.Handle("/metrics", promhttp.Handler())
			http.ListenAndServe(":2112", nil)
		}()
		workerMain(*workerID)
	} else {
		fmt.Println("Invalid mode or missing worker ID")
		os.Exit(1)
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	ch.Close()
	conn.Close()
	exportMetrics() // Final export on shutdown
}
