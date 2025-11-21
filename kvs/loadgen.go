package kvs

import (
	"fmt"
	"math"
	"math/rand/v2"
)

// TransactionWorkload interface for generating transactions
type TransactionWorkload interface {
	NextTransaction() Transaction
}

type Workload struct {
	records       uint64            // Number of records in the key-value store.
	recordSize    uint64            // Size of each record in bytes.
	readThreshold uint64            // Threshold for read operations.
	gen           *Xorshift64       // Random number generator.
	keygen        *ZipfianGenerator // Generator for record selection.
}

func NewWorkload(name string, theta float64) *Workload {
	gen := NewXorshift64(rand.Uint64())
	workload := &Workload{
		records:       1000000, // Default number of records.
		recordSize:    100,     // Default record size in bytes.
		readThreshold: 0,
		gen:           gen,
		keygen:        newZipfianGenerator(1000000, theta, gen),
	}

	readProbability := 0.0

	switch name {
	case "YCSB-A":
		readProbability = 0.50
	case "YCSB-B":
		readProbability = 0.95
	case "YCSB-C":
		readProbability = 1
	default:
		panic("Unknown workload type: " + name)
	}

	workload.readThreshold = uint64(float64(readProbability) * float64(^uint64(0)))

	return workload
}

type WorkloadOp struct {
	Key    uint64 // Key for the operation.
	IsRead bool   // True if this is a read operation, false for write.
}

func (w *Workload) Next() WorkloadOp {
	key := w.keygen.Uint64() % w.records
	isRead := w.gen.Uint64() < w.readThreshold
	return WorkloadOp{Key: key, IsRead: isRead}
}

// Transaction operation types
type TxnOpType int

const (
	TxnGet TxnOpType = 0
	TxnPut TxnOpType = 1
)

// Transaction types
type TxnType int

const (
	RegularTxn      TxnType = 0
	PaymentTxn      TxnType = 1
	VerificationTxn TxnType = 2
)

// Single operation within a transaction
type TxnOperation struct {
	OpType TxnOpType
	Key    uint64
	Value  string // Only used for Put operations
}

// Transaction containing multiple operations
type Transaction struct {
	Operations      []TxnOperation
	TxnType         TxnType // Type of transaction (Regular, Payment, Verification)
	IsPayment       bool    // If true, this is a payment transaction requiring balance checks (DEPRECATED - use TxnType)
	Amount          uint64  // Additional metadata for specific transaction types
	AccountBalances []uint64 // Used by verification transactions to store account balances
}

// =============================================================================
// YCSB Transaction Workload (3-operation transactions based on YCSB patterns)
// =============================================================================

// Generate a 3-operation transaction using WorkloadOp
func (yw *Workload) NextTransaction() Transaction {
	ops := make([]TxnOperation, 3)

	for i := 0; i < 3; i++ {
		baseOp := yw.Next()

		if baseOp.IsRead {
			ops[i] = TxnOperation{
				OpType: TxnGet,
				Key:    baseOp.Key,
			}
		} else {
			ops[i] = TxnOperation{
				OpType: TxnPut,
				Key:    baseOp.Key,
				Value:  fmt.Sprintf("value-%d-%d", baseOp.Key, rand.Uint64()%1000),
			}
		}
	}

	return Transaction{
		Operations: ops,
		TxnType:    RegularTxn,
		IsPayment:  false,
	}
}

// =============================================================================
// Payment Transaction Workload (bank account transfers)
// =============================================================================

type PaymentWorkload struct {
	gen *Xorshift64
}

// =============================================================================
// Verification Transaction Workload (sum verification of accounts 0-9)
// =============================================================================

type VerificationWorkload struct {
	gen *Xorshift64
}

func NewPaymentWorkload() *PaymentWorkload {
	return &PaymentWorkload{
		gen: NewXorshift64(rand.Uint64()),
	}
}

func NewVerificationWorkload() *VerificationWorkload {
	return &VerificationWorkload{
		gen: NewXorshift64(rand.Uint64()),
	}
}

// Generate a payment transaction (2 accounts, transfer money)
func (pw *PaymentWorkload) NextTransaction() Transaction {
	fromAccount := pw.gen.Uint64() % 10
	toAccount := pw.gen.Uint64() % 10

	// Ensure we don't transfer to the same account
	for toAccount == fromAccount {
		toAccount = pw.gen.Uint64() % 10
	}

	// Transfer amount between $1 and $100
	amount := (pw.gen.Uint64() % 100) + 1

	ops := []TxnOperation{
		{OpType: TxnGet, Key: fromAccount},            // Read source balance
		{OpType: TxnGet, Key: toAccount},              // Read dest balance
		{OpType: TxnPut, Key: fromAccount, Value: ""}, // Will be set during execution
		{OpType: TxnPut, Key: toAccount, Value: ""},   // Will be set during execution
	}

	return Transaction{
		Operations: ops,
		TxnType:    PaymentTxn,
		IsPayment:  true,
		Amount:     amount, // Store transfer amount
	}
}

// Generate a verification transaction that sums accounts 0-9 and expects total of 10000
func (vw *VerificationWorkload) NextTransaction() Transaction {
	// Create operations to read all accounts 0-9
	ops := make([]TxnOperation, 10)

	for i := 0; i < 10; i++ {
		ops[i] = TxnOperation{
			OpType: TxnGet,
			Key:    uint64(i),
		}
	}

	return Transaction{
		Operations: ops,
		TxnType:    VerificationTxn,
		IsPayment:  false,    // This is a verification transaction, not a payment
		Amount:     10000000, // Expected total sum
	}
}

// Taken from Wikipedia.
type Xorshift64 struct {
	a uint64 // The state of the generator.;
}

func NewXorshift64(seed uint64) *Xorshift64 {
	if seed == 0 { // The state must be initialized to non-zero.
		seed = 1
	}
	return &Xorshift64{a: seed}
}

func (rng *Xorshift64) Uint64() uint64 {
	x := rng.a
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	rng.a = x
	return x
}

/**
 * Taken from RAMCloud.
 *
 * Used to generate zipfian distributed random numbers where the distribution is
 * skewed toward the lower integers; e.g. 0 will be the most popular, 1 the next
 * most popular, etc.
 *
 * This class implements the core algorithm from YCSB's ZipfianGenerator; it, in
 * turn, uses the algorithm from "Quickly Generating Billion-Record Synthetic
 * Databases", Jim Gray et al, SIGMOD 1994.
 */
type ZipfianGenerator struct {
	gen   *Xorshift64 // Random number generator.
	n     uint64      // Range of numbers to be generated.
	theta float64     // Parameter of the zipfian distribution.
	alpha float64     // Special intermediate result used for generation.
	zetan float64     // Special intermediate result used for generation.
	eta   float64     // Special intermediate result used for generation.
}

/**
 * Construct a generator.  This may be expensive if n is large.
 *
 * \param n
 *      The generator will output random numbers between 0 and n-1.
 * \param theta
 *      The zipfian parameter where 0 < theta < 1 defines the skew; the
 *      smaller the value the more skewed the distribution will be. Default
 *      value of 0.99 comes from the YCSB default value.
 */
func newZipfianGenerator(n uint64, theta float64, gen *Xorshift64) *ZipfianGenerator {
	zetan := zeta(n, theta)
	return &ZipfianGenerator{
		gen:   gen,
		n:     n,
		theta: theta,
		alpha: 1 / (1 - theta),
		zetan: zetan,
		eta:   (1 - math.Pow(2.0/float64(n), 1-theta)) / (1 - zeta(2, theta)/zetan),
	}
}

func newZipfianGeneratorDefaultSkew(n uint64, gen *Xorshift64) *ZipfianGenerator {
	return newZipfianGenerator(n, 0.99, gen)
}

/**
 * Return the zipfian distributed random number between 0 and n-1.
 */
func (g *ZipfianGenerator) Uint64() uint64 {
	u := float64(g.gen.Uint64()) / float64(^uint64(0)) // Normalize to [0, 1).
	uz := u * g.zetan
	if uz < 1 {
		return 0
	}
	if uz < 1+math.Pow(0.5, g.theta) {
		return 1
	}
	return 0 + uint64(float64(g.n)*math.Pow(g.eta*u-g.eta+1.0, g.alpha))
}

/**
 * Returns the nth harmonic number with parameter theta; e.g. H_{n,theta}.
 */
func zeta(n uint64, theta float64) float64 {
	sum := 0.0
	for i := uint64(0); i < n; i++ {
		sum = sum + 1.0/(math.Pow(float64(i+1), theta))
	}
	return sum
}
