package jobs

import "dbsync/sync/core"

//Service represents a job service
type JobService interface {
	//List lists all active or recently active jobs
	List(request *ListRequest) *ListResponse
	//Create creates a new job
	Create(ID string) *core.Job
	//Get returns a job for supplied ID or nil
	Get(ID string) *core.Job
}

type jobService struct {
	registry *registry
}

//Get returns job by ID or nil
func (s *jobService) Get(ID string) *core.Job {
	jobs := s.registry.list()
	for i := range jobs {
		if jobs[i].ID == ID {
			jobs[i].Update()
			return jobs[i]
		}
	}
	return nil

}

//List lists all jobs
func (s *jobService) List(request *ListRequest) *ListResponse {
	jobs := s.registry.list()
	if len(request.IDs) == 0 {
		return &ListResponse{
			Jobs: jobs,
		}
	}
	var requestedIDs = make(map[string]bool)
	for i := range request.IDs {
		requestedIDs[request.IDs[i]] = true
	}
	var filtered = make([]*core.Job, 0)
	for i := range jobs {
		if _, has := requestedIDs[jobs[i].ID]; !has {
			continue
		}
		jobs[i].Update()
		filtered = append(filtered, jobs[i])
	}
	return &ListResponse{
		Jobs: filtered,
	}
}

//Create creates a new job
func (s *jobService) Create(ID string) *core.Job {
	job := core.NewJob(ID)
	s.registry.add(job)
	return job
}

//New create a job service
func New() JobService {
	return &jobService{
		registry: newRegistry(),
	}
}
