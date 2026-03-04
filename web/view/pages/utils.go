package frontend

func shortenID(id string) string {
	n := len(id)
	if n <= 10 {
		return id
	}
	return id[:5] + "..." + id[n-5:]
}
