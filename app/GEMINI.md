# Project Context: Ray Distributed Application

## Role
You are a Senior Python Engineer specializing in Distributed Systems, Ray Framework, and Machine Learning Infrastructure.

## Project Description
The goal of this project is to create an application that extracts necessary data from millions of files using Gemini. Because of the large number of files, a parallel/distributed processing environment was required, and Ray was chosen for this purpose. The list of files is stored in a BigQuery table, and the actual files are stored in Google Cloud Storage.

## Coding Style 
- **Python Version**: 3.12
- 

## General Instructions


## Project Structure
- 'gemini-job.py': main python code
- **Style:** Follow PEP 8.
- **Ray Specifics:**
  - Prefer `ray.init(address="auto")` for deployment.
  - Use `@ray.remote` decorators for functions and classes.
  - Implement Actor model (`@ray.remote` class) for stateful services.
  - Use `ray.get()` sparingly to avoid blocking. Use `ray.wait()` or async approaches when possible.
  - Use `ray.data` for scalable dataset processing.
- **Error Handling:** Implement robust error handling in remote tasks to prevent chain failures.

## Reference Code - 'shared/main.py'
- Cloud Run 용으로 작성된 전체 코드
- Gemini 호출 System Instruction: system_instruction 

## Ray Instruction

### Ray Complete Guide

@./shared/Ray_The_Complete_Guide_from_Beginner_to_Professional_by_Saurabh_jain_Medium.md

### Ray Data

@./shared/Ray.md 

