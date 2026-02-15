package sstable

import (
	"encoding/binary"
	"hash/fnv"
)

type bloom struct {
	m uint32 // 总位数 (bit 数)
	k uint8  // 哈希函数次数
	b []byte // 实际存储位的字节数组
}

func newBloom(m uint32, k uint8) *bloom {
	if m < 8 {
		m = 8
	}
	nbytes := (m + 7) / 8

	return &bloom{m: m, k: k, b: make([]byte, nbytes)}
}

// add 将一个 Key（字符串）添加到布隆过滤器中。
// 在位数组（bitset）中选出 k 个位置，并将这些位置对应的 bit 置为 1。
func (bf *bloom) add(key string) {
	// 1) 生成第一个基础哈希值 h1
	h1 := fnv64a(key)

	// 2) 生成第二个哈希值 h2
	// 0x9e3779b97f4a7c15 是黄金分割数相关的常数，用来打乱 h1 的位分布。
	h2 := mix64(h1 ^ 0x9e3779b97f4a7c15)

	// 3) 执行 k 次打点（Double Hashing）
	// 根据数学证明，使用 h = h1 + i * h2 模拟出的 k 个位置，其随机分布的效果与 k 个独立哈希函数几乎一致。
	for i := uint8(0); i < bf.k; i++ {
		h := h1 + uint64(i)*h2
		pos := uint32(h % uint64(bf.m))
		bf.set(pos)
	}
}

// 判断 key 是否“可能存在”。
func (bf *bloom) mayContain(key string) bool {
	h1 := fnv64a(key)
	h2 := mix64(h1 ^ 0x9e3779b97f4a7c15)

	for i := uint8(0); i < bf.k; i++ {
		h := h1 + uint64(i)*h2
		pos := uint32(h % uint64(bf.m))
		if !bf.get(pos) {
			return false
		}
	}
	return true
}

func (bf *bloom) set(i uint32) {
	byteIdx := i / 8
	bit := uint8(i % 8)
	bf.b[byteIdx] |= 1 << bit
}

func (bf *bloom) get(i uint32) bool {
	byteIdx := i / 8
	bit := uint8(i % 8)
	return (bf.b[byteIdx] & (1 << bit)) != 0
}

// 序列化格式：| m(uint32) | k(uint8) | pad(3B) | bitsetLen(uint32) | bitset... |
func (bf *bloom) marshal() []byte {
	out := make([]byte, 4+1+3+4+len(bf.b))
	binary.LittleEndian.PutUint32(out[0:4], bf.m)
	out[4] = bf.k
	// out[5:8] pad = 0
	binary.LittleEndian.PutUint32(out[8:12], uint32(len(bf.b)))
	copy(out[12:], bf.b)
	return out
}

// 从字节流反序列化 bloom。
func unmarshalBloom(p []byte) (*bloom, bool) {
	if len(p) < 12 {
		return nil, false
	}

	m := binary.LittleEndian.Uint32(p[0:4])
	k := p[4]
	if m == 0 || k == 0 {
		return nil, false
	}
	n := binary.LittleEndian.Uint32(p[8:12])
	if int(12+n) != len(p) || n == 0 {
		return nil, false
	}
	b := make([]byte, n)
	copy(b, p[12:])

	return &bloom{m: m, k: k, b: b}, true
}

// FNV-1a 算法，把字符串变成一个基础的 64 位数字。
func fnv64a(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

// 扰动函数，通过位移和乘法确保 h2 和 h1 之间没有明显的数学关联。
func mix64(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}
