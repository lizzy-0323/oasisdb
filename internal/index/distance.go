package index

import "math"

func distance(a, b []float32, space SpaceType) float32 {
    switch space {
    case IPSpace:
        // inner product (−dot product to convert to distance)
        var dot float32
        for i := range a {
            dot += a[i] * b[i]
        }
        return -dot
    case CosSpace:
        var dot, na, nb float32
        for i := range a {
            dot += a[i] * b[i]
            na += a[i] * a[i]
            nb += b[i] * b[i]
        }
        if na == 0 || nb == 0 {
            return 1.0 // maximal distance
        }
        return 1 - dot/float32(math.Sqrt(float64(na*nb)))
    case HammingSpace:
        var hamming float32
        for i := range a {
            if a[i] != b[i] {
                hamming++
            }
        }
        return hamming
    default: // L2
        var sum float32
        for i := range a {
            diff := a[i] - b[i]
            sum += diff * diff
        }
        return sum
    }
}