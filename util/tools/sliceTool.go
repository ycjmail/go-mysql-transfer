package tools

import (
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type reducetype func(interface{}) interface{}
type filtertype func(interface{}) bool

func SliceRandList(min, max int) []int {
	if max < min {
		min, max = max, min
	}
	length := max - min + 1
	t0 := time.Now()
	rand.Seed(int64(t0.Nanosecond()))
	list := rand.Perm(length)
	for index, _ := range list {
		list[index] += min
	}
	return list
}

func SliceMerge(slice1, slice2 []interface{}) (c []interface{}) {
	c = append(slice1, slice2...)
	return
}

func SliceMergeMap(s ...[]map[string]interface{}) (slice []map[string]interface{}) {
	switch len(s) {
	case 0:
		break
	case 1:
		slice = s[0]
		break
	default:
		s1 := s[0]
		s2 := SliceMergeMap(s[1:]...) //...将数组元素打散
		slice = make([]map[string]interface{}, len(s1)+len(s2))
		copy(slice, s1)
		copy(slice[len(s1):], s2)
		break
	}
	return slice
}

func SliceContain(val interface{}, slice []interface{}) bool {
	for _, v := range slice {
		if v == val {
			return true
		}
	}
	return false
}

//字符串数组中是否有val元素
func InStringSlice(val interface{}, slice []string) bool {
	for _, v := range slice {
		if v == val {
			return true
		}
	}
	return false
}

func SliceReduce(slice []interface{}, a reducetype) (dslice []interface{}) {
	for _, v := range slice {
		dslice = append(dslice, a(v))
	}
	return
}

func SliceRand(a []interface{}) (b interface{}) {
	randnum := rand.Intn(len(a))
	b = a[randnum]
	return
}

func SliceRandMap(s []map[string]interface{}, length int) []map[string]interface{} {
	rand.Seed(time.Now().Unix())
	a := make([]map[string]interface{}, len(s))
	copy(a, s)
	for i := len(a) - 1; i > 0; i-- {
		num := rand.Intn(i + 1)
		a[i], a[num] = a[num], a[i]
	}
	//b := make([]map[string]interface{},0)
	//for i := 0; i < length; i++ {
	//	b = append(b,a[i])
	//}
	return a[:length]
}
func SliceRandString(a []string, length int) string {
	rand.Seed(time.Now().Unix())
	for i := len(a) - 1; i > 0; i-- {
		num := rand.Intn(i + 1)
		a[i], a[num] = a[num], a[i]
	}
	b := ""
	for i := 0; i < length; i++ {
		b += a[i]
	}
	return b
}

func FindFirstByMap(s []map[string]interface{}, q map[string]interface{}) map[string]interface{} {
	if q == nil {
		return nil
	}
	for _, m := range s {
		isMatch := true
		for k, v := range q {
			if m[k] != v {
				isMatch = false
				break
			}
		}
		if isMatch {
			return m
		}
	}
	return nil
}

func FindAllByMap(s []map[string]interface{}, q map[string]interface{}) []map[string]interface{} {
	records := make([]map[string]interface{}, 0)
	if q == nil {
		return records
	}

	for _, m := range s {
		isMatch := true
		for k, v := range q {
			if m[k] != v {
				isMatch = false
				break
			}
		}
		if isMatch {
			records = append(records, m)
		}
	}
	return records
}

func SlicePluck(s []map[string]interface{}, key string) []interface{} {
	records := make([]interface{}, 0)
	for _, m := range s {
		records = append(records, m[key])
	}
	return records
}

func SliceImplode(s []interface{}, delimit string) string {
	str := ""
	for _, m := range s {
		if str != "" {
			str += delimit
		}
		str += m.(string)
	}
	return str
}

func SliceSum(intslice []int64) (sum int64) {
	for _, v := range intslice {
		sum += v
	}
	return
}

func SliceFilter(slice []interface{}, a filtertype) (ftslice []interface{}) {
	for _, v := range slice {
		if a(v) {
			ftslice = append(ftslice, v)
		}
	}
	return
}

func SliceDiff(slice1, slice2 []interface{}) (diffslice []interface{}) {
	for _, v := range slice1 {
		if !InSlice(v, slice2) {
			diffslice = append(diffslice, v)
		}
	}
	return
}

func InSlice(val interface{}, slice []interface{}) bool {
	for _, v := range slice {
		if v == val {
			return true
		}
	}
	return false
}

func SliceIntersect(slice1, slice2 []interface{}) (diffslice []interface{}) {
	for _, v := range slice1 {
		if !InSlice(v, slice2) {
			diffslice = append(diffslice, v)
		}
	}
	return
}

func SliceChunk(slice []interface{}, size int) (chunkslice [][]interface{}) {
	if size >= len(slice) {
		chunkslice = append(chunkslice, slice)
		return
	}
	end := size
	for i := 0; i <= (len(slice) - size); i += size {
		chunkslice = append(chunkslice, slice[i:end])
		end += size
	}
	return
}

func SliceRange(start, end, step int64) (intslice []int64) {
	for i := start; i <= end; i += step {
		intslice = append(intslice, i)
	}
	return
}

func SlicePad(slice []interface{}, size int, val interface{}) []interface{} {
	if size <= len(slice) {
		return slice
	}
	for i := 0; i < (size - len(slice)); i++ {
		slice = append(slice, val)
	}
	return slice
}

func SliceUnique(slice []interface{}) (uniqueslice []interface{}) {
	for _, v := range slice {
		if !InSlice(v, uniqueslice) {
			uniqueslice = append(uniqueslice, v)
		}
	}
	return
}

func SliceShuffle(slice []interface{}) []interface{} {
	for i := 0; i < len(slice); i++ {
		a := rand.Intn(len(slice))
		b := rand.Intn(len(slice))
		slice[a], slice[b] = slice[b], slice[a]
	}
	return slice
}

func SliceRemoveRepeat(slice []uint) []uint {
	var res []uint
	bymap := make(map[uint]uint)
	for _, value := range slice {
		if _, ok := bymap[value]; !ok {
			bymap[value] = value
		}
	}
	for _, v := range bymap {
		res = append(res, v)
	}
	return res
}

//逗号分隔的id字符串转成数组
func SliceString2Ids(seq string) []uint {
	stringIds := strings.Split(seq, ",")
	ids := make([]uint, 0)
	for _, v := range stringIds {
		i, _ := strconv.Atoi(v)
		if i == 0 {
			continue
		}
		ids = append(ids, uint(i))
	}
	return ids
}

func SliceIds2String(ids []uint) string {
	seq := ""
	for key, value := range ids {
		if key != len(ids)-1 {
			seq = seq + strconv.Itoa(int(value)) + ","
		} else {
			seq = seq + strconv.Itoa(int(value))
		}
	}
	return seq
}
func SliceContains(val interface{}, ids []uint) bool {
	for _, v := range ids {
		if v == val {
			return true
		}
	}
	return false
}

//字符串数组用逗号合成一个字符串
func SliceStrings2String(val []string) string {
	res := ""
	for key, value := range val {
		if key != len(val)-1 {
			res = res + value + ","
		} else {
			res = res + value
		}
	}
	return res
}

//字符串用逗号分隔成一个字符串数组
func SliceString2Strings(val string) []string {
	strs := strings.Split(val, ",")
	res := make([]string, 0)
	for _, v := range strs {
		res = append(res, v)
	}
	return res
}

// ArrayContain .
func ArrayContain(obj interface{}, target interface{}) (bool, error) {
	targetValue := reflect.ValueOf(target)
	switch reflect.TypeOf(target).Kind() {
	case reflect.Slice, reflect.Array:
		for i := 0; i < targetValue.Len(); i++ {
			if targetValue.Index(i).Interface() == obj {
				return true, nil
			}
		}
	case reflect.Map:
		if targetValue.MapIndex(reflect.ValueOf(obj)).IsValid() {
			return true, nil
		}
	}
	return false, nil
}
func SliceIds2SqlCond(ids []uint) string {
	var s string
	if len(ids) > 0 {
		s = "park_lot_id in ("
		s += SliceIds2String(ids) + ")"
	} else {
		s = "park_lot_id = -1"
	}
	return s
}

func UintsContains(array []uint, val uint) bool {
	for i := 0; i < len(array); i++ {
		if array[i] == val {
			return true
		}
	}
	return false
}
