package pipeline

import (
	"fmt"
	"strconv"
	"strings"
)

/*
id,gender,age,hypertension,heart_disease,ever_married,work_type,Residence_type,avg_glucose_level,bmi,smoking_status,stroke
9046,Male,67,0,1,Yes,Private,Urban,228.69,36.6,formerly smoked,1
51676,Female,61,0,0,Yes,Self-employed,Rural,202.21,N/A,never smoked,1
31112,Male,80,0,1,Yes,Private,Rural,105.92,32.5,never smoked,1
60182,Female,49,0,0,Yes,Private,Urban,171.23,34.4,smokes,1
1665,Female,79,1,0,Yes,Self-employed,Rural,174.12,24,never smoked,1
56669,Male,81,0,0,Yes,Private,Urban,186.21,29,formerly smoked,1
53882,Male,74,1,1,Yes,Private,Rural,70.09,27.4,never smoked,1
10434,Female,69,0,0,No,Private,Urban,94.39,22.8,never smoked,1
27419,Female,59,0,0,Yes,Private,Rural,76.15,N/A,Unknown,1
60491,Female,78,0,0,Yes,Private,Urban,58.57,24.2,Unknown,1
12109,Female,81,1,0,Yes,Private,Rural,80.43,29.7,never smoked,1
12095,Female,61,0,1,Yes,Govt_job,Rural,120.46,36.8,smokes,1
*/

const (
	id = iota
	gender
	age
	hypertension
	heart_disease
	ever_married
	work_type
	Residence_type
	avg_glucose_level
	bmi
	smoking_status
	stroke
)

/*
Note: In real life, use protocol buffers and wrap marshallers around them.

This makes the data object useful in any language and puts the defs in a centralized place.
*/

type SmokingStatus int8

const (
	SmokingStatusUnknown SmokingStatus = 0
	Smoked               SmokingStatus = 1
	NeverSmoked          SmokingStatus = 2
	Smokes               SmokingStatus = 3
	FormerlySmoked       SmokingStatus = 4
)

func (s *SmokingStatus) Unmarshal(str string) error {
	switch str {
	case "smoked":
		*s = Smoked
		return nil
	case "never smoked":
		*s = NeverSmoked
		return nil
	case "smokes":
		*s = Smokes
		return nil
	case "formerly smoked":
		*s = FormerlySmoked
		return nil
	case "Unknown":
		return nil
	}
	return fmt.Errorf("SmokingStatus(%s) is an unknown type", str)
}

type WorkType int8

const (
	WorkTypeUnknown WorkType = 0
	Private         WorkType = 1
	SelfEmployed    WorkType = 2
	Govtjob         WorkType = 3
	Parent          WorkType = 4
	NeverEmployed   WorkType = 5
)

func (w *WorkType) Unmarshal(s string) error {
	switch s {
	case "Private":
		*w = Private
	case "Self-employed":
		*w = SelfEmployed
	case "Govt_job":
		*w = Govtjob
	case "children":
		*w = Parent
	case "Never_worked":
		*w = NeverEmployed
	default:
		return fmt.Errorf("WorkType(%s) is an unknown value", s)
	}
	return nil
}

type Residence int8

const (
	ResidenceUnknown Residence = 0
	Urban            Residence = 1
	Rural            Residence = 2
)

func (r *Residence) Unmarshal(s string) error {
	switch s {
	case "Urban":
		*r = Urban
	case "Rural":
		*r = Rural
	default:
		return fmt.Errorf("Residence(%s) is an unknown value", s)
	}
	return nil
}

type Sex int8

const (
	SexUnknown Sex = 0
	Male       Sex = 1
	Female     Sex = 2
	SexOther       = 3
)

type Conditions struct {
	HyperTension  bool
	HeartDisease  bool
	GlucoseLevel  float64
	BMI           float64
	SmokingStatus SmokingStatus
	PrevStroke    bool
}

func (c *Conditions) Unmarshal(line []string) error {
	b, err := boolConv(line[hypertension])
	if err != nil {
		return fmt.Errorf("HyperTension: %s", err)
	}
	c.HyperTension = b

	b, err = boolConv(line[heart_disease])
	if err != nil {
		return fmt.Errorf("HeartDisease: %s", err)
	}
	c.HeartDisease = b

	f, err := strconv.ParseFloat(line[avg_glucose_level], 64)
	if err != nil {
		return fmt.Errorf("GlucoseLevel(%s): %s", line[avg_glucose_level], err)
	}
	c.GlucoseLevel = f

	if line[bmi] != "N/A" {
		f, err = strconv.ParseFloat(line[bmi], 64)
		if err != nil {
			return fmt.Errorf("BMI(%s): %s", line[bmi], err)
		}
		c.BMI = f
	}

	if err = c.SmokingStatus.Unmarshal(line[smoking_status]); err != nil {
		return err
	}

	d, err := strconv.Atoi(line[stroke])
	if err != nil {
		return fmt.Errorf("Stroke(%s): %s", line[stroke], err)
	}
	switch d {
	case 0:
	case 1:
		c.PrevStroke = true
	default:
		return fmt.Errorf("PrevStroke was %d, which we don't recognize", d)
	}

	return nil
}

func boolConv(s string) (bool, error) {
	b, err := strconv.Atoi(s)
	if err != nil {
		return false, fmt.Errorf("HyperTension(%s) was not a int", s)
	}
	switch b {
	case 0:
		return false, nil
	case 1:
		return true, nil
	}
	return false, fmt.Errorf("(%s) was not a 0 or 1", s)
}

type Social struct {
	Married   bool
	WorkType  WorkType
	Residence Residence
}

func (s *Social) Unmarshal(line []string) error {
	str := line[ever_married]
	switch str {
	case "Yes":
		s.Married = true
	case "No":
	default:
		return fmt.Errorf("Married(%s) is an unknown value", str)
	}

	if err := s.WorkType.Unmarshal(line[work_type]); err != nil {
		return err
	}

	return s.Residence.Unmarshal(line[Residence_type])
}

type Victim struct {
	ID  int
	Sex Sex
	Age float64

	Conditions Conditions
	Social     Social
}

func (v *Victim) Unmarshal(line []string) (Victim, error) {
	d, err := strconv.Atoi(line[id])
	if err != nil {
		return Victim{}, fmt.Errorf("id(%s) was not a number", line[id])
	}
	v.ID = d

	s := line[gender]
	switch s {
	case "Male":
		v.Sex = Male
	case "Female":
		v.Sex = Female
	case "Other":
		v.Sex = SexOther
	default:
		return Victim{}, fmt.Errorf("gender(%s) was unknown vlaue", line[gender])
	}

	f, err := strconv.ParseFloat(line[age], 64)
	if err != nil {
		return Victim{}, fmt.Errorf("age(%s) was not a number: %v", line[age], strings.Join(line, ", "))
	}
	v.Age = f

	social := Social{}
	if err := social.Unmarshal(line); err != nil {
		return Victim{}, err
	}
	v.Social = social

	conditions := Conditions{}
	if err := conditions.Unmarshal(line); err != nil {
		return Victim{}, err
	}
	v.Conditions = conditions
	return *v, nil
}

type Stats struct {
	Victims                   int32
	Males                     int32
	Females                   int32
	MalesWithGreaterThanOne   int32
	FemalesWithGreaterThanOne int32
	PercentMalesMarried       float64
	PrecentFemalesMarried     float64

	malesMarried   int32
	femalesMarried int32
}

func (s *Stats) Finalize() {
	s.PercentMalesMarried = (float64(s.malesMarried) / float64(s.Males)) * 100
	s.PrecentFemalesMarried = (float64(s.femalesMarried) / float64(s.Females)) * 100
}
