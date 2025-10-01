@echo off
REM This script initializes a Git repository, connects it to GitHub, and pushes all files.

REM Set the commit message
set "commitMessage=Initial commit of AI caller scripts"

echo =======================================================
echo            GitHub Upload Script
echo =======================================================

REM Change to your project directory
cd /d "C:\Users\james\OneDrive\Downloads\Desktop\Summit\Current Scripts"

REM Initialize a new Git repository (if it's not already one)
IF NOT EXIST .git (
    echo --- Initializing new Git repository...
    git init
) ELSE (
    echo --- Git repository already exists.
)

REM Set the main branch name to "main"
git branch -M main

REM Add the remote repository (will show an error if it already exists, which is safe to ignore)
echo --- Adding remote GitHub repository...
git remote add origin https://github.com/jadenterprises/AI_Callers_For_Deals_2.git

REM Add all files in the folder to the staging area
echo --- Staging all files...
git add .

REM Commit the files with your message
echo --- Committing files...
git commit -m "%commitMessage%"

REM Push the files to your GitHub repository
echo --- Pushing files to GitHub...
git push -u origin main

echo.
echo --- 🚀 Process complete! Your files have been uploaded. ---
pause