package transport

import (
	"lamina/internal/catalog"
	"net/http"
)

func (s *Server) createTemporary(w http.ResponseWriter, r *http.Request) {
	var in catalog.Relation
	if !decode(w, r, &in) {
		return
	}
	out, err := s.db.CreateTemporary(r.PathValue("tx"), in)
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 201, out)
}
func (s *Server) dropTemporary(w http.ResponseWriter, r *http.Request) {
	if err := s.db.DropTemporary(r.PathValue("tx"), r.PathValue("name")); err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, map[string]string{"state": "dropped"})
}
func (s *Server) listTemporary(w http.ResponseWriter, r *http.Request) {
	out, err := s.db.ListTemporary(r.PathValue("tx"))
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, map[string]any{"temporary": out})
}
